/*
 * Copyright 2017-2018 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* eslint no-eval: 0 */

'use strict'

const fs = require('fs')
const { minify } = require('uglify-es')
const openwhisk = require('openwhisk')
const os = require('os')
const path = require('path')

// read conductor version number
const version = require('./package.json').version

// synthesize conductor action code from composition
function synthesize ({ name, composition, ast, version: composer, annotations = [] }) {
  const code = `// generated by composer v${composer} and conductor v${version}\n\nconst composition = ${JSON.stringify(composition, null, 4)}\n\n// do not edit below this point\n\n` +
    minify(`const main=(${main})(composition)`, { output: { max_line_len: 127 } }).code
  annotations = annotations.concat([{ key: 'conductor', value: ast }, { key: 'composerVersion', value: composer }, { key: 'conductorVersion', value: version }])
  return { name, action: { exec: { kind: 'nodejs:default', code }, annotations } }
}

// return enhanced openwhisk client capable of deploying compositions
module.exports = function (options) {
  // try to extract apihost and key first from whisk property file file and then from process.env
  let apihost
  let apikey

  try {
    const wskpropsPath = process.env.WSK_CONFIG_FILE || path.join(os.homedir(), '.wskprops')
    const lines = fs.readFileSync(wskpropsPath, { encoding: 'utf8' }).split('\n')

    for (let line of lines) {
      let parts = line.trim().split('=')
      if (parts.length === 2) {
        if (parts[0] === 'APIHOST') {
          apihost = parts[1]
        } else if (parts[0] === 'AUTH') {
          apikey = parts[1]
        }
      }
    }
  } catch (error) { }

  if (process.env.__OW_API_HOST) apihost = process.env.__OW_API_HOST
  if (process.env.__OW_API_KEY) apikey = process.env.__OW_API_KEY

  const wsk = openwhisk(Object.assign({ apihost, api_key: apikey }, options))
  wsk.compositions = new Compositions(wsk)
  return wsk
}

// management class for compositions
class Compositions {
  constructor (wsk) {
    this.actions = wsk.actions
  }

  deploy (composition, overwrite) {
    const actions = (composition.actions || []).concat(synthesize(composition))
    return actions.reduce((promise, action) => promise.then(() => overwrite && this.actions.delete(action).catch(() => { }))
      .then(() => this.actions.create(action)), Promise.resolve())
      .then(() => actions)
  }
}

// runtime code
function main (composition) {
  const openwhisk = require('openwhisk')
  let wsk

  const isObject = obj => typeof obj === 'object' && obj !== null && !Array.isArray(obj)

  // compile ast to fsm
  const compiler = {
    sequence (parent, node) {
      return [{ parent, type: 'pass' }, ...compile(parent, ...node.components)]
    },

    action (parent, node) {
      return [{ parent, type: 'action', name: node.name }]
    },

    async (parent, node) {
      const body = [...compile(parent, ...node.components)]
      return [{ parent, type: 'async', return: body.length + 2 }, ...body, { parent, type: 'stop' }, { parent, type: 'pass' }]
    },

    function (parent, node) {
      return [{ parent, type: 'function', exec: node.function.exec }]
    },

    finally (parent, node) {
      const finalizer = compile(parent, node.finalizer)
      const fsm = [{ parent, type: 'try' }, ...compile(parent, node.body), { parent, type: 'exit' }, ...finalizer]
      fsm[0].catch = fsm.length - finalizer.length
      return fsm
    },

    let (parent, node) {
      return [{ parent, type: 'let', let: node.declarations }, ...compile(parent, ...node.components), { parent, type: 'exit' }]
    },

    mask (parent, node) {
      return [{ parent, type: 'let', let: null }, ...compile(parent, ...node.components), { parent, type: 'exit' }]
    },

    try (parent, node) {
      const handler = [...compile(parent, node.handler), { parent, type: 'pass' }]
      const fsm = [{ parent, type: 'try' }, ...compile(parent, node.body), { parent, type: 'exit' }, ...handler]
      fsm[0].catch = fsm.length - handler.length
      fsm[fsm.length - handler.length - 1].next = handler.length
      return fsm
    },

    if_nosave (parent, node) {
      const consequent = compile(parent, node.consequent)
      const alternate = [...compile(parent, node.alternate), { parent, type: 'pass' }]
      const fsm = [{ parent, type: 'pass' }, ...compile(parent, node.test), { parent, type: 'choice', then: 1, else: consequent.length + 1 }, ...consequent, ...alternate]
      fsm[fsm.length - alternate.length - 1].next = alternate.length
      return fsm
    },

    while_nosave (parent, node) {
      const body = compile(parent, node.body)
      const fsm = [{ parent, type: 'pass' }, ...compile(parent, node.test), { parent, type: 'choice', then: 1, else: body.length + 1 }, ...body, { parent, type: 'pass' }]
      fsm[fsm.length - 2].next = 2 - fsm.length
      return fsm
    },

    dowhile_nosave (parent, node) {
      const fsm = [{ parent, type: 'pass' }, ...compile(parent, node.body), ...compile(parent, node.test), { parent, type: 'choice', else: 1 }, { parent, type: 'pass' }]
      fsm[fsm.length - 2].then = 2 - fsm.length
      return fsm
    }
  }

  function compile (parent, node) {
    if (arguments.length === 1) return [{ parent, type: 'empty' }]
    if (arguments.length === 2) return Object.assign(compiler[node.type](node.path || parent, node), { path: node.path })
    return Array.prototype.slice.call(arguments, 1).reduce((fsm, node) => { fsm.push(...compile(parent, node)); return fsm }, [])
  }

  const fsm = compile('', composition)

  const conductor = {
    choice ({ p, node, index }) {
      p.s.state = index + (p.params.value ? node.then : node.else)
    },

    try ({ p, node, index }) {
      p.s.stack.unshift({ catch: index + node.catch })
    },

    let ({ p, node, index }) {
      p.s.stack.unshift({ let: JSON.parse(JSON.stringify(node.let)) })
    },

    exit ({ p, node, index }) {
      if (p.s.stack.length === 0) return internalError(`pop from an empty stack`)
      p.s.stack.shift()
    },

    action ({ p, node, index }) {
      return { method: 'action', action: node.name, params: p.params, state: { $resume: p.s } }
    },

    function ({ p, node, index }) {
      return Promise.resolve().then(() => run(node.exec.code, p))
        .catch(error => {
          console.error(error)
          return { error: `Function combinator threw an exception at AST node root${node.parent} (see log for details)` }
        })
        .then(result => {
          if (typeof result === 'function') result = { error: `Function combinator evaluated to a function type at AST node root${node.parent}` }
          // if a function has only side effects and no return value, return params
          p.params = JSON.parse(JSON.stringify(result === undefined ? p.params : result))
          inspect(p)
          return step(p)
        })
    },

    empty ({ p, node, index }) {
      inspect(p)
    },

    pass ({ p, node, index }) {
    },

    async ({ p, node, index, inspect, step }) {
      p.params.$resume = { state: p.s.state, stack: [{ marker: true }].concat(p.s.stack) }
      p.s.state = index + node.return
      if (!wsk) wsk = openwhisk()
      return wsk.actions.invoke({ name: process.env.__OW_ACTION_NAME, params: p.params })
        .then(response => ({ method: 'async', activationId: response.activationId, sessionId: p.s.session }), error => {
          console.error(error) // invoke failed
          return { error: `Async combinator failed to invoke composition at AST node root${node.parent} (see log for details)` }
        })
        .then(result => {
          p.params = result
          inspect(p)
          return step(p)
        })
    },

    stop ({ p, node, index, inspect, step }) {
      p.s.state = -1
    }
  }

  function finish (q) { // using p here causes issues with minimist!
    return q.params.error ? q.params : { params: q.params }
  }

  const internalError = error => Promise.reject(error) // terminate composition execution and record error

  // wrap params if not a dictionary, branch to error handler if error
  function inspect (p) {
    if (!isObject(p.params)) p.params = { value: p.params }
    if (p.params.error !== undefined) {
      p.params = { error: p.params.error } // discard all fields but the error field
      p.s.state = -1 // abort unless there is a handler in the stack
      while (p.s.stack.length > 0 && !p.s.stack[0].marker) {
        if ((p.s.state = p.s.stack.shift().catch || -1) >= 0) break
      }
    }
  }

  // run function f on current stack
  function run (f, p) {
    // handle let/mask pairs
    const view = []
    let n = 0
    for (let frame of p.s.stack) {
      if (frame.let === null) {
        n++
      } else if (frame.let !== undefined) {
        if (n === 0) {
          view.push(frame)
        } else {
          n--
        }
      }
    }

    // update value of topmost matching symbol on stack if any
    function set (symbol, value) {
      const element = view.find(element => element.let !== undefined && element.let[symbol] !== undefined)
      if (element !== undefined) element.let[symbol] = JSON.parse(JSON.stringify(value))
    }

    // collapse stack for invocation
    const env = view.reduceRight((acc, cur) => cur.let ? Object.assign(acc, cur.let) : acc, {})
    let main = '(function(){try{const require=arguments[2];'
    for (const name in env) main += `var ${name}=arguments[1]['${name}'];`
    main += `return eval((function(){return(${f})})())(arguments[0])}finally{`
    for (const name in env) main += `arguments[1]['${name}']=${name};`
    main += '}})'
    try {
      return (1, eval)(main)(p.params, env, require)
    } finally {
      for (const name in env) set(name, env[name])
    }
  }

  function step (p) {
    // final state, return composition result
    if (p.s.state < 0 || p.s.state >= fsm.length) {
      console.log(`Entering final state`)
      console.log(JSON.stringify(p.params))
      return
    }

    // process one state
    const node = fsm[p.s.state] // json definition for index state
    if (node.path !== undefined) console.log(`Entering composition${node.path}`)
    const index = p.s.state // current state
    p.s.state = p.s.state + (node.next || 1) // default next state
    if (typeof conductor[node.type] !== 'function') return internalError(`unexpected "${node.type}" combinator`)
    return conductor[node.type]({ p, index, node, inspect, step }) || step(p)
  }

  // do invocation
  return (params) => {
    // extract parameters
    const $resume = params.$resume || {}
    delete params.$resume
    $resume.session = $resume.session || process.env.__OW_ACTIVATION_ID

    // current state
    const p = { s: Object.assign({ state: 0, stack: [], resuming: true }, $resume), params }

    // step and catch all errors
    return Promise.resolve().then(() => {
      if (typeof p.s.state !== 'number') return internalError('state parameter is not a number')
      if (!Array.isArray(p.s.stack)) return internalError('stack parameter is not an array')

      if ($resume.resuming) inspect(p) // handle error objects when resuming

      return step(p)
    }).catch(error => {
      const message = (typeof error.error === 'string' && error.error) || error.message || (typeof error === 'string' && error)
      p.params = { error: message ? `Internal error: ${message}` : 'Internal error' }
    }).then(params => params || finish(p)) // params is defined iff execution will be resumed
  }
}
