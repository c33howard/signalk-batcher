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
                return regexes.some(function(re) { return value.path.search(re) != -1; });
            } else {
                // if we're filtering to exclude, we'll include this in the
                // result if every regex doesn't match (ie search finds
                // nothing)
                return regexes.every(function(re) { return value.path.search(re) == -1; });
            }
        };
    };

    // do a full object nested traversal of obj and call emit(path, obj) for
    // every key=name, where path is the json notation of the key in the root
    // object and obj is the found value at that key
    //
    // ie
    //  {
    //      'a': 1,
    //      'b': {
    //          'a': 2
    //      }
    //  }
    //
    // will call emit('a', 1) and emit('a.b', 2)
    let _visit = function(obj, name, emit) {
        let _do_visit = function(obj, name, path, emit) {
            Object.keys(obj).forEach(key => {
                const key_path = path != '' ? `${path}.${key}`: key;
                const value = obj[key];

                if (key === name) {
                    emit(key_path, value);
                }

                if (value && typeof(value) == 'object') {
                    _do_visit(value, name, key_path, emit);
                }
            });
        };

        return _do_visit(obj, name, '', emit);
    };

    let _add_kv_to_batch = function(batch_of_points, path, value) {
        if (!batch_of_points.data[path]) {
            batch_of_points.data[path] = [];
        }

        trace(`add to batch: ${path} ${value}`);
        batch_of_points.data[path].push(value);
    };

    let _add_current_state_to_batch = function(options) {
        // construct the filter function once and use the result
        let filter_function = _construct_filter_function(options);

        return function(batch_of_points, state) {
            // TODO: only handles self for now
            const vessel = state.vessels[app.selfId];

            batch_of_points.header.push(Date.now());

            // for each key in the vessel, descend until we find a value
            _visit(vessel, 'value', function(path, value) {
                // trim ".value" from the suffix of the path
                path = path.replace(/\.value$/, '');

                // skip anything filtered out
                if (!filter_function(path)) {
                    return;
                }

                // TODO: probably want to filter out data points that have a
                // long expired timestamp

                if (typeof(value) === 'object') {
                    _.forIn(value, function(v, k) {
                        _add_kv_to_batch(batch_of_points, `${path}.${k}`, v);
                    });
                } else {
                    _add_kv_to_batch(batch_of_points, path, value);
                }
            });
        };
    };

    let _create_update_interval = function(options, publish_callback) {
        let _reset_batch = function() {
            return {
                header: [],
                data: []
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
        _publish_interval = setInterval(function() {
            debug(`_publish`);

            // publish
            publish_callback(batch_of_points);

            // reset the batch of points
            batch_of_points = _reset_batch();
        }, options.write_interval * 1000);

        debug(`write_interval=${options.write_interval}`);

        // periodically get the total state of signalk
        // TODO: figure out what to do with data points that come and/or go
        // within an update interval, which is most likely due to a device
        // being turned on or off, or a plugin being stopped or started
        _update_interval = setInterval(function() {
            add_to_batch(batch_of_points, app.signalk.retrieve());
        }, options.update_interval * 1000);
    };

    /**
     *  options: {
     *      'filter_list_type': 'include|exclude',
     *      'filter_list': [<path glob>],
     *      // how often we get the state of signalk
     *      'update_interval': seconds,
     *      // how often we invoke the publish callback with a batch of points
     *      'publish_interval': seconds
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
     */
    let _start = function(options, publish_callback) {
        // TODO: figure out plugin init order, which is relevant when we do our
        // first retrieve(), but not all plugins have produced their first data
        // point yet
        _create_update_interval(options, publish_callback);
    };

    let _stop = function(options) {
        clearInterval(_publish_interval);
        clearInterval(_update_interval);

        _publish_interval = undefined;
        _update_interval = undefined;
    };

    return {
        start: _start,
        stop: _stop
    };
};
