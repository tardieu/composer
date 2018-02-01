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

'use strict'

// composer module

const fs = require('fs')
const os = require('os')
const path = require('path')
const util = require('util')
const clone = require('clone')
const openwhisk = require('openwhisk')

class ComposerError extends Error {
    constructor(message, cause) {
        super(message)
        if (typeof cause !== 'undefined') {
            const index = this.stack.indexOf('\n')
            this.stack = this.stack.substring(0, index) + '\nCause: ' + util.inspect(cause) + this.stack.substring(index)
        }
    }
}

// build a composition with a single state
function singleton(entry, Manifest = []) {
    return { entry, states: [entry], exit: entry, Manifest }
}

// chain two compositions, mutating front
function chain(front, back) {
    if (front.type) front = singleton(front)
    if (back.type) back = singleton(back)
    front.states.push(...back.states)
    front.exit.next = back.entry
    front.exit = back.exit
    front.Manifest.push(...back.Manifest)
    return front
}

class Composer {
    constructor(options = {}) {
        // try to extract apihost and key from wskprops
        let apihost
        let api_key

        try {
            const wskpropsPath = process.env.WSK_CONFIG_FILE || path.join(os.homedir(), '.wskprops')
            const lines = fs.readFileSync(wskpropsPath, { encoding: 'utf8' }).split('\n')

            for (let line of lines) {
                let parts = line.trim().split('=')
                if (parts.length === 2) {
                    if (parts[0] === 'APIHOST') {
                        apihost = parts[1]
                    } else if (parts[0] === 'AUTH') {
                        api_key = parts[1]
                    }
                }
            }
        } catch (error) { }

        this.wsk = openwhisk(Object.assign({ apihost, api_key }, options))
        this.seq = this.sequence
    }

    task(obj) {
        if (arguments.length > 1) throw new ComposerError('Too many arguments')
        if (obj == null) {
            // case null: identity function (must throw errors if any)
            return this.function(params => params, { helper: 'null' })
        } else if (Array.isArray(obj) && obj.length > 0 && typeof obj.slice(-1)[0].name === 'string') {
            // case array: last action in the array
            return singleton({ type: 'action', action: obj.slice(-1)[0].name }, clone(obj))
        } else if (typeof obj === 'object' && typeof obj.entry === 'object' && Array.isArray(obj.states) && typeof obj.exit === 'object' && Array.isArray(obj.Manifest)) {
            // case object: composition
            return clone(obj)
        } else if (typeof obj === 'function') {
            // case function: inline function
            return this.function(obj)
        } else if (typeof obj === 'string') {
            // case string: action
            return this.action(obj)
        } else {
            // error
            throw new ComposerError('Invalid argument', obj)
        }
    }

    sequence() { // varargs
        if (arguments.length == 0) return this.task()
        return Array.prototype.map.call(arguments, x => this.task(x), this).reduce(chain)
    }

    if(test, consequent, alternate, options = {}) {
        if (arguments.length > 4) throw new ComposerError('Too many arguments')
        if (typeof options !== 'object' || options === null) throw new ComposerError('Invalid argument', options)
        const id = {}
        test = options.nosave ? this.task(test) : chain({ type: 'push', id }, this.task(test))
        consequent = options.nosave ? this.task(consequent) : chain({ type: 'pop', id, branch: 'then' }, this.task(consequent))
        alternate = options.nosave ? this.task(alternate) : chain({ type: 'pop', id, branch: 'else' }, this.task(alternate))
        const exit = { type: 'pass', id }
        const choice = { type: 'choice', then: consequent.entry, else: alternate.entry, id }
        test.states.push(choice)
        test.states.push(...consequent.states)
        test.states.push(...alternate.states)
        test.states.push(exit)
        test.exit.next = choice
        consequent.exit.next = exit
        alternate.exit.next = exit
        test.exit = exit
        test.Manifest.push(...consequent.Manifest)
        test.Manifest.push(...alternate.Manifest)
        return test
    }

    while(test, body, options = {}) {
        if (arguments.length > 3) throw new ComposerError('Too many arguments')
        if (typeof options !== 'object' || options === null) throw new ComposerError('Invalid argument', options)
        const id = {}
        test = options.nosave ? this.task(test) : chain({ type: 'push', id }, this.task(test))
        const consequent = options.nosave ? this.task(body) : chain({ type: 'pop', id, branch: 'then' }, this.task(body))
        const exit = options.nosave ? { type: 'pass', id } : { type: 'pop', id, branch: 'else' }
        const choice = { type: 'choice', then: consequent.entry, else: exit, id }
        test.states.push(choice)
        test.states.push(...consequent.states)
        test.states.push(exit)
        test.exit.next = choice
        consequent.exit.next = test.entry
        test.exit = exit
        test.Manifest.push(...consequent.Manifest)
        return test
    }

    try(body, handler) {
        if (arguments.length > 2) throw new ComposerError('Too many arguments')
        const id = {}
        handler = this.task(handler)
        body = [{ type: 'enter', id, frame: { catch: handler.entry } }, this.task(body), { type: 'exit', id }].reduce(chain)
        const exit = { type: 'pass', id }
        body.states.push(...handler.states)
        body.states.push(exit)
        body.exit.next = exit
        handler.exit.next = exit
        body.exit = exit
        body.Manifest.push(...handler.Manifest)
        return body
    }

    finally(body, handler) {
        if (arguments.length > 2) throw new ComposerError('Too many arguments')
        const id = {}
        handler = this.task(handler)
        return [{ type: 'enter', id, frame: { catch: handler.entry } }, this.task(body), { type: 'exit', id }, handler].reduce(chain)
    }

    let(obj) { // varargs
        if (typeof obj !== 'object' || obj === null) throw new ComposerError('Invalid argument', obj)
        const id = {}
        return [{ type: 'enter', id, frame: { let: JSON.parse(JSON.stringify(obj)) } }, this.sequence(...Array.prototype.slice.call(arguments, 1)), { type: 'exit', id }].reduce(chain)
    }

    value(v) {
        if (arguments.length > 1) throw new ComposerError('Too many arguments')
        if (typeof v === 'function') throw new ComposerError('Invalid argument', v)
        return singleton({ type: 'value', value: typeof v === 'undefined' ? {} : JSON.parse(JSON.stringify(v)) })

    }

    function(f, options = {}) {
        if (arguments.length > 2) throw new ComposerError('Too many arguments')
        if (typeof options !== 'object' || options === null) throw new ComposerError('Invalid argument', options)
        if (typeof f === 'function') {
            const code = `${f}`
            if (code.indexOf('[native code]') !== -1) throw new ComposerError('Cannot capture native function', f)
            f = code
        }
        if (typeof f !== 'string') throw new ComposerError('Invalid argument', f)
        return singleton({ type: 'function', function: f, helper: options.helper })

    }

    action(name, options = {}) {
        if (arguments.length > 2) throw new ComposerError('Too many arguments')
        if (typeof options !== 'object' || options === null) throw new ComposerError('Invalid argument', options)
        if (typeof name !== 'string') throw new ComposerError('Invalid argument', name)
        let Manifest = []
        if (options.filename) { // read action code from file
            Manifest = [{ name, action: fs.readFileSync(options.filename, { encoding: 'utf8' }) }]
        } else if (options.sequence) { // native sequence
            const components = options.sequence.map(a => a.indexOf('/') == -1 ? `/_/${a}` : a)
            Manifest = [{ name, action: { exec: { kind: 'sequence', components } } }]
        } else if (typeof options.action === 'string' || typeof options.action === 'object' && options.action !== null) {
            Manifest = [{ name, action: options.action }]
        } else if (typeof options.action === 'function') {
            const action = `${options.action}`
            if (action.indexOf('[native code]') !== -1) throw new ComposerError('Cannot capture native function', options.action)
            Manifest = [{ name, action }]
        }
        return singleton( { type: 'action', action: name })
    }

    retain(body, options = {}) {
        if (arguments.length > 2) throw new ComposerError('Too many arguments')
        if (typeof options !== 'object' || options === null) throw new ComposerError('Invalid argument', options)
        if (typeof options.filter === 'function') {
            // return { params: filter(params), result: body(params) }
            return this.sequence(this.retain(options.filter),
                this.retain(this.finally(this.function(({ params }) => params, { helper: 'retain_4' }), body), { field: 'result', catch: options.catch }))
        }
        if (options.catch) {
            // return { params, result: body(params) } even if result is an error
            return this.sequence(
                this.retain(
                    this.try(
                        this.sequence(body, this.function(result => ({ result }), { helper: 'retain_1' })),
                        this.function(result => ({ result }), { helper: 'retain_3' })),
                    { field: options.field }),
                this.function(({ params, result }) => ({ params, result: result.result }), { helper: 'retain_2' }))
        } else {
            // return { params, result: body(params) } if no error, otherwise body(params)
            const id = {}
            return [{ type: 'push', id, field: options.field }, this.task(body), { type: 'pop', id, collect: true }].reduce(chain)
        }
    }

    repeat(count) { // varargs
        if (typeof count !== 'number') throw new ComposerError('Invalid argument', count)
        return this.let({ count }, this.while(this.function(() => count-- > 0, { helper: 'repeat_1' }), this.sequence(...Array.prototype.slice.call(arguments, 1))))
    }

    retry(count) { // varargs
        if (typeof count !== 'number') throw new ComposerError('Invalid argument', count)
        const attempt = this.retain(this.sequence(...Array.prototype.slice.call(arguments, 1)), { catch: true })
        return this.let({ count },
            attempt,
            this.while(
                this.function(({ result }) => typeof result.error !== 'undefined' && count-- > 0, { helper: 'retry_1' }),
                this.finally(this.function(({ params }) => params, { helper: 'retry_2' }), attempt)),
            this.function(({ result }) => result, { helper: 'retry_3' }))
    }

    // produce action code
    compile(name, obj, filename) {
        if (arguments.length > 3) throw new ComposerError('Too many arguments')
        if (typeof name !== 'string') throw new ComposerError('Invalid argument', name)
        if (typeof filename !== 'undefined' && typeof filename !== 'string') throw new ComposerError('Invalid argument', filename)
        obj = this.task(obj)
        const states = {}
        let entry
        let exit
        let count = 0
        obj.states.forEach(state => {
            if (typeof state.id === 'undefined') state.id = {}
            if (typeof state.id.id === 'undefined') state.id.id = count++
            const id = state.type + '_' + (state.branch ? state.branch + '_' : '') + state.id.id
            states[id] = state
            state.id = id
            if (state === obj.entry) entry = id
            if (state === obj.exit) exit = id
        })
        obj.states.forEach(state => {
            if (state.next) state.next = state.next.id
            if (state.then) state.then = state.then.id
            if (state.else) state.else = state.else.id
            if (state.frame && state.frame.catch) state.frame.catch = state.frame.catch.id
        })
        obj.states.forEach(state => delete state.id)
        const composition = { entry, states, exit }
        const action = `const __eval__ = main => eval(main)\n${main}\nconst __composition__ = ${JSON.stringify(composition, null, 4)}\n`
        if (filename) fs.writeFileSync(filename, action, { encoding: 'utf8' })
        obj.Manifest.push({ name, action, annotations: { conductor: composition } })
        return obj.Manifest
    }

    // deploy actions, return count of successfully deployed actions
    deploy(actions) {
        if (arguments.length > 1) throw new ComposerError('Too many arguments')
        if (!Array.isArray(actions)) throw new ComposerError('Invalid argument', array)
        // clone array as openwhisk mutates the actions
        return clone(actions).reduce(
            (promise, action) => promise.then(i => this.wsk.actions.update(action).then(_ => i + 1, err => { console.error(err); return i })), Promise.resolve(0))
    }
}

module.exports = options => new Composer(options)

// conductor action

function main(params) {
    const isObject = obj => typeof obj === 'object' && obj !== null && !Array.isArray(obj)

    // encode error object
    const encodeError = error => ({
        code: typeof error.code === 'number' && error.code || 500,
        error: (typeof error.error === 'string' && error.error) || error.message || (typeof error === 'string' && error) || 'An internal error occurred'
    })

    // error status codes
    const badRequest = error => Promise.reject({ code: 400, error })
    const internalError = error => Promise.reject(encodeError(error))

    // catch all
    return Promise.resolve().then(() => invoke(params)).catch(internalError)

    // do invocation
    function invoke(params) {
        // initial state and stack
        let state = __composition__.entry
        let stack = []

        // check parameters
        if (typeof __composition__.entry !== 'string') return badRequest('The composition has no entry field of type string')
        if (!isObject(__composition__.states)) return badRequest('The composition has no states field of type object')
        if (typeof __composition__.exit !== 'string') return badRequest('The composition has no exit field of type string')

        // restore state and stack when resuming
        if (typeof params.$resume !== 'undefined') {
            if (!isObject(params.$resume)) return badRequest('The type of optional $resume parameter must be object')
            state = params.$resume.state
            stack = params.$resume.stack
            if (typeof state !== 'undefined' && typeof state !== 'string') return badRequest('The type of optional $resume.state parameter must be string')
            if (!Array.isArray(stack)) return badRequest('The type of $resume.stack must be an array')
            delete params.$resume
            inspect() // handle error objects when resuming
        }

        // wrap params if not a dictionary, branch to error handler if error
        function inspect() {
            if (!isObject(params)) params = { value: params }
            if (typeof params.error !== 'undefined') {
                params = { error: params.error } // discard all fields but the error field
                state = undefined // abort unless there is a handler in the stack
                while (stack.length > 0) {
                    if (state = stack.shift().catch) break
                }
            }
        }

        // run function f on current stack
        function run(f) {
            // update value of topmost matching symbol on stack if any
            function set(symbol, value) {
                const element = stack.find(element => typeof element.let !== 'undefined' && typeof element.let[symbol] !== 'undefined')
                if (typeof element !== 'undefined') element.let[symbol] = JSON.parse(JSON.stringify(value))
            }

            // collapse stack for invocation
            const env = stack.reduceRight((acc, cur) => typeof cur.let === 'object' ? Object.assign(acc, cur.let) : acc, {})
            let main = '(function main(){try{'
            for (const name in env) main += `var ${name}=arguments[1]['${name}'];`
            main += `return eval((${f}))(arguments[0])}finally{`
            for (const name in env) main += `arguments[1]['${name}']=${name};`
            main += '}})'
            try {
                return __eval__(main)(params, env)
            } finally {
                for (const name in env) set(name, env[name])
            }
        }

        while (true) {
            // final state, return composition result
            if (!state) {
                console.log(`Entering final state`)
                console.log(JSON.stringify(params))
                if (params.error) return params; else return { params }
            }

            // process one state
            console.log(`Entering ${state}`)

            if (!isObject(__composition__.states[state])) return badRequest(`State ${state} definition is missing`)
            const json = __composition__.states[state] // json definition for current state
            const current = state // current state for error messages
            if (json.type !== 'choice' && typeof json.next !== 'string' && state !== __composition__.exit) return badRequest(`State ${state} has no next field`)
            state = json.next // default next state

            switch (json.type) {
                case 'choice':
                    if (typeof json.then !== 'string') return badRequest(`State ${current} has no then field`)
                    if (typeof json.else !== 'string') return badRequest(`State ${current} has no else field`)
                    state = params.value === true ? json.then : json.else
                    break
                case 'enter':
                    if (!isObject(json.frame)) return badRequest(`State ${current} specified an invalid frame`)
                    stack.unshift(json.frame)
                    break
                case 'exit':
                    if (stack.length === 0) return badRequest(`State ${current} attempted to pop from an empty stack`)
                    stack.shift()
                    break
                case 'push':
                    if (typeof json.field !== 'undefined' && typeof json.field !== 'string') return badRequest(`State ${current} is invalid`)
                    stack.unshift(JSON.parse(JSON.stringify({ params: json.field ? params[json.field] : params })))
                    break
                case 'pop':
                    if (stack.length === 0) return badRequest(`State ${current} attempted to pop from an empty stack`)
                    if (typeof json.collect !== 'undefined' && typeof json.collect !== 'boolean') return badRequest(`State ${current} is invalid`)
                    params = json.collect ? { params: stack.shift().params, result: params } : stack.shift().params
                    break
                case 'action':
                    if (typeof json.action !== 'string') return badRequest(`State ${current} specifies an invalid action`)
                    return { action: json.action, params, state: { $resume: { state, stack } } } // invoke continuation
                    break
                case 'value':
                    if (typeof json.value === 'undefined') return badRequest(`State ${current} specifies an invalid value`)
                    params = json.value
                    inspect()
                    break
                case 'function':
                    if (typeof json.function !== 'string') return badRequest(`State ${current} specifies an invalid function`)
                    let result
                    try {
                        result = run(json.function)
                    } catch (error) {
                        console.error(error)
                        result = { error: `An exception was caught at state ${current} (see log for details)` }
                    }
                    if (typeof result === 'function') result = { error: `State ${current} evaluated to a function` }
                    // if a function has only side effects and no return value, return params
                    params = JSON.parse(JSON.stringify(typeof result === 'undefined' ? params : result))
                    inspect()
                    break
                case 'pass':
                    break
                default:
                    return badRequest(`State ${current} has an unknown type`)
            }
        }
    }
}
