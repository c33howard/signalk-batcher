/*
 * Copyright 2020 Craig Howard <craig@choward.ca>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const debug = require('debug')('signalk-to-batch-points');
const trace = require('debug')('signalk-to-batch-points:trace');
const _ = require('lodash');

module.exports = function(app) {
    let _publish_interval;
    let _update_interval;

    let _construct_filter_function = function(options) {
        const regexes = options.filter_list.map(function(path) {
            // path is a glob pattern, and we need to convert it to a regex matcher
            let regex = path;
            // first convert '.' to '\.'
            regex = regex.replace(/\./gi, '\\.');
            // next convert '*' to '.*'
            regex = regex.replace(/\*/gi, '.*');
            // finally always do a full match
            regex = `^${regex}$`;
            trace(`created regex=${regex} from path=${path}`);
            // and create the regex
            return new RegExp(regex, 'g');
        });

        return function(value) {
            // always filter out design, as nothing there varies by time
            if (value.startsWith('design.')) {
                return false;
            }
            // always filter out notifications, as they're a separate channel
            if (value.startsWith('notifications.')) {
                return false;
            }

            // TODO: it might be more efficient to create a single giant regex
            // on startup than to do .some() or .every()
            if (options.filter_list_type == 'include') {
                // if we're filtering to include elements, we'll include if at
                // least one regex matches (ie, the search finds something)
                return regexes.some(function(re) { return value.search(re) != -1; });
            } else {
                // if we're filtering to exclude, we'll include this in the
                // result if every regex doesn't match (ie search finds
                // nothing)
                return regexes.every(function(re) { return value.search(re) == -1; });
            }
        };
    };

    // visits all nodes in obj (descending down the object tree), and calls
    // emit() for each object that has a property of 'name'.
    //
    // ie:
    //  {
    //      a: {
    //          n: { 1: 2 },
    //          b: {
    //              n: 3
    //          }
    //      }
    //  }
    //
    // when called _visit(obj, 'n', emit), this function will invoke emit:
    //
    //  emit('a.n', { 1: 2 })
    //  emit('a.b.n', 3)
    let _visit = function(obj, name, emit) {
        let _do_visit = function(obj, name, path, emit) {
            Object.keys(obj).forEach(key => {
                const key_path = path != '' ? `${path}.${key}`: key;
                const value = obj[key];

                // if this object has a propery that matches name, it's the one
                // we're looking for, so emit it
                if (_.has(value, name)) {
                    emit(key_path, value);
                }

                // if this is an object, recurse into it
                if (value && typeof(value) == 'object') {
                    _do_visit(value, name, key_path, emit);
                }
            });
        };

        return _do_visit(obj, name, '', emit);
    };

    let _add_kv_to_batch = function(batch_of_points, path, source, value) {
        // sigh, it would be *so* much nicer if everything was just points, so
        // convert from objects to points where we're forced to
        if (typeof(value) === 'object') {
            _.forIn(value, function(v, k) {
                _add_kv_to_batch(batch_of_points, `${path}.${k}`, source, v);
            });

            return;
        }

        // relying on a text delimiter is a bit lame, but this really is a
        // composite key, as it's valid to have the same path from two
        // different sources
        const key = `${path}|${source}`;

        if (!batch_of_points.data[key]) {
            batch_of_points.data[key] = [];
        }

        trace(`add to batch: ${path} ${source} ${value}`);
        batch_of_points.data[key].push(value);
    };

    let _add_current_state_to_batch = function(options) {
        // construct the filter function once and use the result
        let filter_function = _construct_filter_function(options);

        return function(batch_of_points, state) {
            // TODO: only handles self for now
            const vessel = state.vessels[app.selfId];

            batch_of_points.header.push(options.now());

            // for each key in the vessel, descend until we find something
            // which a $source (which everything will have, since the full
            // model always uses the pointer method
            _visit(vessel, '$source', function(path, value) {
                // skip anything filtered out
                if (!filter_function(path)) {
                    return;
                }

                const source = value['$source'];

                // TODO: probably want to filter out data points that have a
                // long expired timestamp

                _add_kv_to_batch(batch_of_points, path, source, value.value);

                // if there are multiple sources providing a value, they'll be
                // in a nested key called "values" (plural), which is a dict,
                // and the key is the $source, and the value is an object with
                // "value" and "timestamp"
                if (value.values) {
                    // we've already emitted the primary value, so skip it
                    const secondary_values = _.omit(value.values, source);
                    _.forIn(secondary_values, function(v, source) {
                        _add_kv_to_batch(batch_of_points, path, source, v.value);
                    });
                }
            });
        };
    };

    let _set_interval = function(interval, callback) {
        if (typeof(interval) === 'number') {
            return setInterval(callback, interval * 1000);
        } else {
            return interval(callback);
        }
    };

    let _clear_interval = function(interval, token) {
        if (typeof(interval) === 'number') {
            clearInterval(token);
        }
    };

    let _create_update_interval = function(options, publish_callback) {
        let _reset_batch = function() {
            return {
                header: [],
                data: {}
            };
        };

        // construct the filter function once and use the result
        const add_to_batch = _add_current_state_to_batch(options);

        // cache the points here for a batch upload
        // key = signalk path, value = value
        // this batch has the last value registered during an interval and
        // that's what will be published to timestream
        let batch_of_points = _reset_batch();

        // periodically publish the batched metrics to timestream
        _publish_interval = _set_interval(options.write_interval, function() {
            debug(`_publish`);

            // publish
            publish_callback(batch_of_points);

            // reset the batch of points
            batch_of_points = _reset_batch();
        });

        // periodically get the total state of signalk
        // TODO: figure out what to do with data points that come and/or go
        // within an update interval, which is most likely due to a device
        // being turned on or off, or a plugin being stopped or started
        _update_interval = _set_interval(options.update_interval, function() {
            add_to_batch(batch_of_points, app.signalk.retrieve());
        });
    };

    /**
     *  options: {
     *      'filter_list_type': 'include|exclude',
     *      'filter_list': [<path glob>],
     *      // how often we get the state of signalk
     *      'update_interval': seconds|function,
     *      // how often we invoke the publish callback with a batch of points
     *      'publish_interval': seconds|function
     *  }
     *
     *  publish_callback: function(batch_of_points)
     *  where: batch_of_points = {
     *      name: <string> [{
     *          name: <string>
     *          value: <string>|<number>|<object>
     *          timestamp: <Date>
     *      }]
     *  }
     *
     *  If update_interval or publish_interval are seconds, then we'll use
     *  setInterval to ensure that things run on this period.  If they're a
     *  function, we will not setup a setInterval, and it's up to the caller to
     *  invoke those methods periodically to make forward progress.  This is
     *  useful for unit tests (see the tests for example usage).
     */
    let _start = function(options, publish_callback) {
        if (!options.now) {
            options.now = Date.now;
        }

        // TODO: figure out plugin init order, which is relevant when we do our
        // first retrieve(), but not all plugins have produced their first data
        // point yet
        _create_update_interval(options, publish_callback);
    };

    let _stop = function(options) {
        _clear_interval(_publish_interval);
        _clear_interval(_update_interval);

        _publish_interval = undefined;
        _update_interval = undefined;
    };

    return {
        start: _start,
        stop: _stop
    };
};
