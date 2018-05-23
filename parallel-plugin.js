'use strict'

class ParallelPlugin {
    combinators() {
        return {
            parallel: { components: { named: true }, since: '0.6.0' },
            map: { args: [{ _: 'task', named: true }], since: '0.6.0' },
        }
    }

    compiler() {
        return {
            parallel(node) {
                return [{ type: 'parallel', components: node.components, path: node.path }]
            },

            map(node) {
                return [{ type: 'map', task: node.task, path: node.path }]
            },
        }
    }

    conductor() {
        const openwhisk = require('openwhisk')
        let wsk

        return {
            parallel({ p, node, index, inspect, step }) {
                if (!wsk) wsk = openwhisk({ ignore_certs: true })
                return Promise.all(node.components.map((task, index) =>
                    wsk.actions.invoke({ name: task.name, params: p.params, blocking: !task.async })
                        .then(activation => task.async ? activation : activation.response.result)))
                    .catch(error => {
                        console.error(error)
                        return { error: `An exception was caught at state ${index} (see log for details)` }
                    })
                    .then(result => {
                        p.params = result
                        inspect(p)
                        return step(p)
                    })
            },

            map({ p, node, index, inspect, step }) {
                if (!wsk) wsk = openwhisk({ ignore_certs: true })
                return Promise.all(p.params.value.map((value, index) =>
                    wsk.actions.invoke({ name: node.task.name, params: Object.assign(p.params, { value: undefined }, value), blocking: !node.task.async })
                        .then(activation => node.task.async ? activation : activation.response.result)))
                    .catch(error => {
                        console.error(error)
                        return { error: `An exception was caught at state ${index} (see log for details)` }
                    })
                    .then(result => {
                        p.params = result
                        inspect(p)
                        return step(p)
                    })
            },
        }
    }
}

module.exports = new ParallelPlugin()
