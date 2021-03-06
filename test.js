const chai = require('chai');
const should = chai.should();
const _ = require('lodash');

const batcher = require('.');

// this function is required (rather than using require() directly), so
// that we can modify the data in one test and then get clean data again in
// the next test
let load_data_from_disk = function(filename) {
    const loaded_data = require(filename);
    return _.cloneDeep(loaded_data);
};

describe('to-batch', function() {
    const to = batcher.to_batch;

    let _test_data;
    let bp_instance;

    const trigger = function() {
        let _callback;

        return {
            trigger: function() {
                _callback();
            },
            init: function(callback) {
                _callback = callback;
            }
        };
    };

    let get_interval = trigger();
    let publish_interval = trigger();

    let publish = function() {
        let _last_batch = {};

        return {
            publish: function(batch_of_points) {
                _last_batch = batch_of_points;
            },
            last: function() {
                return _last_batch;
            }
        }
    }();

    let update_data = function(_test_data, path, millis, value) {
        const vessel = _test_data.vessels[_test_data.self.split('.')[1]];
        _.set(vessel, `${path}.timestamp`, new Date(millis).toISOString());
        _.set(vessel, `${path}.value`, value);
    };

    let _now;

    beforeEach(function() {
        _now = 1606688510443;
    });

    const init = function(options) {
        const mock_app = {
            selfId: _test_data.self.split('.')[1],
            signalk: {
                retrieve: function() {
                    return _test_data;
                }
            }
        };
        bp_instance = to(mock_app);

        if (!options) options = {};
        if (!options.filter_list_type) options.filter_list_type = 'exclude';
        if (!options.filter_list) options.filter_list = [];

        bp_instance.start({
            publish_interval: publish_interval.init,
            get_interval: get_interval.init,
            now: function() { return _now; },
            filter_list_type: options.filter_list_type,
            filter_list: options.filter_list
        }, publish.publish);
    };

    afterEach(function() {
        bp_instance.stop();
    });

    it('publish-data', function() {
        _test_data = load_data_from_disk('./test-apparent-wind-speed.json');

        init();

        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        wind: {
                            speedApparent: {
                                "test-source": [[0, 0]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('get-twice-no-new-observations', function() {
        _test_data = load_data_from_disk('./test-apparent-wind-speed.json');

        init();

        // get data
        get_interval.trigger();

        // advance time
        _now++;

        // publish
        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        wind: {
                            speedApparent: {
                                // it's the same value and the observation hasn't updated, so omit
                                "test-source": [[0, 0]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('get-twice-new-observations', function() {
        _test_data = load_data_from_disk('./test-apparent-wind-speed.json');

        init();

        // get data
        get_interval.trigger();

        // advance time
        _now++;
        // update the data
        update_data(_test_data, 'environment.wind.speedApparent', _now, 1.2);

        // publish
        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        wind: {
                            speedApparent: {
                                "test-source": [[0, 0], [1, 1.2]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('get-twice-new-same-observation', function() {
        _test_data = load_data_from_disk('./test-apparent-wind-speed.json');

        init();

        // get data
        get_interval.trigger();

        // advance time
        _now++;
        // update the data
        update_data(_test_data, 'environment.wind.speedApparent', _now, 0);

        // publish
        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        wind: {
                            speedApparent: {
                                "test-source": [[0, 0], [1]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('get-thrice-new-same-observation', function() {
        _test_data = load_data_from_disk('./test-apparent-wind-speed.json');

        init();

        // get data
        get_interval.trigger();

        // advance time
        _now++;
        // update the data
        update_data(_test_data, 'environment.wind.speedApparent', _now, 0);

        // get data
        get_interval.trigger();

        // advance time
        _now++;
        // update the data
        update_data(_test_data, 'environment.wind.speedApparent', _now, 0);

        // publish
        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        wind: {
                            speedApparent: {
                                "test-source": [[0, 0], [1], [2]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('publish-two-metrics', function() {
        _test_data = load_data_from_disk('./test-apparent-wind-speed-angle.json');

        init();

        // get data
        get_interval.trigger();

        // advance time
        _now++;
        update_data(_test_data, 'environment.wind.speedApparent', _now, 1.2);
        update_data(_test_data, 'environment.wind.angleApparent', _now, 1.978);

        // publish
        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        wind: {
                            speedApparent: {
                                "test-source": [[0, 0], [1, 1.2]]
                            },
                            angleApparent: {
                                "test-source": [[0, 1.9799], [1, 1.978]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('changing-value', function() {
        _test_data = load_data_from_disk('./test-apparent-wind-speed.json');

        init();

        // get data
        get_interval.trigger();

        // advance time
        _now++;
        // change wind speed
        update_data(_test_data, 'environment.wind.speedApparent', _now, 1.2);

        // publish
        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        wind: {
                            speedApparent: {
                                "test-source": [[0, 0], [1, 1.2]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('reset-batch-after-write', function() {
        _test_data = load_data_from_disk('./test-apparent-wind-speed.json');

        init();

        // get the data
        get_interval.trigger();

        // advance time, which makes the observation older than the new
        // _last_publish_time that we'll get after the publish
        _now += 100;

        // publish
        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        wind: {
                            speedApparent: {
                                "test-source": [[0, 0]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });

        // publish again, no updated data, so this should be empty
        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {}
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.543Z"
        });
    });

    it('publish-data-with-object-values', function() {
        _test_data = load_data_from_disk('./test-navigation-position.json');

        init();

        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    navigation: {
                        position: {
                            latitude: {
                                "test-source": [[0, 47.67]]
                            },
                            longitude: {
                                "test-source": [[0, -122.4]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('ignore-old-data', function() {
        _test_data = load_data_from_disk('./test-apparent-wind-speed-angle.json');

        // start in the future, so what we get from the source is old
        _now += 100;

        init();

        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {}
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.543Z"
        });
    });

    it('include-data-at-end-of-period', function() {
        _test_data = load_data_from_disk('./test-apparent-wind-speed-angle.json');

        // start in the future, so what we get from the source is old
        _now += 100;

        init();

        // get the data on our regular cadence, the data is too old, so it's
        // not included
        get_interval.trigger();

        // get a fresh observation from the source, so we have data that should
        // be published now
        _now += 100;
        update_data(_test_data, 'environment.wind.speedApparent', _now, 1.2);

        // publish, and ensure that the data was added, even though we didn't
        // do another get, because if we don't include it, the next get will
        // consider it too old and we'll never see the data point, which would
        // be a problem for data that we fetch on the same period as our
        // publish interval
        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        wind: {
                            speedApparent: {
                                "test-source": [[100, 1.2]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.543Z"
        });
    });

    it('include-nothing', function() {
        _test_data = load_data_from_disk('./test-apparent-wind-speed-angle.json');

        init({
            filter_list_type: 'include',
            filter_list: []
        });

        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {}
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('include-one-metric', function() {
        _test_data = load_data_from_disk('./test-apparent-wind-speed-angle.json');

        init({
            filter_list_type: 'include',
            filter_list: ['environment.wind.speedApparent']
        });

        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        wind: {
                            speedApparent: {
                                "test-source": [[0, 0]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('exclude-one-metric', function() {
        _test_data = load_data_from_disk('./test-apparent-wind-speed-angle.json');

        init({
            filter_list_type: 'exclude',
            filter_list: ['environment.wind.speedApparent']
        });

        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        wind: {
                            angleApparent: {
                                "test-source": [[0, 1.9799]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('glob-include', function() {
        _test_data = load_data_from_disk('./test-environment-and-navigation.json');

        init({
            filter_list_type: 'include',
            filter_list: ['environment.*']
        });

        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        wind: {
                            speedApparent: {
                                "test-source": [[0, 0]]
                            },
                            angleApparent: {
                                "test-source": [[0, 1.9799]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('glob-exclude', function() {
        _test_data = load_data_from_disk('./test-environment-and-navigation.json');

        init({
            filter_list_type: 'exclude',
            filter_list: ['environment.*']
        });

        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    navigation: {
                        speedThroughWater: {
                            "test-source": [[0, 0]]
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('two-sources', function() {
        _test_data = load_data_from_disk('./test-apparent-wind-speed-two-sources.json');

        init();

        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        wind: {
                            speedApparent: {
                                "test-source-1": [[0, 0]],
                                "test-source-2": [[0, 0.5]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('two-sources-object-value', function() {
        _test_data = load_data_from_disk('./test-navigation-position-two-sources.json');

        init();

        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    navigation: {
                        position: {
                            latitude: {
                                "test-source-1": [[0, 47.67]],
                                "test-source-2": [[0, 47.68]]
                            },
                            longitude: {
                                "test-source-1": [[0, -122.40]],
                                "test-source-2": [[0, -122.50]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('complex-source', function() {
        _test_data = load_data_from_disk('./test-complex-source.json');

        init();

        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        wind: {
                            speedApparent: {
                                "test-source": [[0, 0]]
                            }
                        }
                    }
                }
            },
            sources: {
                "test-source": {
                    key1: 1,
                    key2: 2,
                    key3: {
                        a: 'b',
                        c: 'd'
                    }
                }
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });

    it('test-rpi', function() {
        _test_data = load_data_from_disk('./test-rpi.json');

        init();

        publish_interval.trigger();

        publish.last().should.deep.equal({
            version: "2.0.0",
            self: "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
            vessels: {
                "urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930": {
                    environment: {
                        rpi: {
                            cpu: {
                                temperature: {
                                    "signalk-raspberry-pi-monitoring": [[0, 324.69]]
                                },
                                utilisation: {
                                    "signalk-raspberry-pi-monitoring": [[0, 0.07]]
                                },
                                core: {
                                    1: {
                                        utilisation: {
                                            "signalk-raspberry-pi-monitoring": [[0, 0.06]]
                                        }
                                    },
                                    2: {
                                        utilisation: {
                                            "signalk-raspberry-pi-monitoring": [[0, 0.08]]
                                        }
                                    },
                                    3: {
                                        utilisation: {
                                            "signalk-raspberry-pi-monitoring": [[0, 0.08]]
                                        }
                                    },
                                    4: {
                                        utilisation: {
                                            "signalk-raspberry-pi-monitoring": [[0, 0.06]]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            sources: {
                "signalk-raspberry-pi-monitoring": {}
            },
            timestamp: "2020-11-29T22:21:50.443Z"
        });
    });
});

describe('from-batch', function() {
    const from = batcher.from_batch;

    it('test-basic-load', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688510443,
                value: 2.43
            },
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688515443,
                value: 2.44
            },
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688520443,
                value: 2.41
            },
        ]);
    });

    it('test-unchanged-value', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed-unchanged.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688510443,
                value: 2.43
            },
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688515443,
                value: 2.43
            },
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688520443,
                value: 2.43
            },
        ]);
    });

    it('test-unchanged-then-changed-value', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed-unchanged-then-changed.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688510443,
                value: 2.43
            },
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688515443,
                value: 2.43
            },
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688520443,
                value: 2.5
            },
        ]);
    });

    it('test-initial-zero', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed-initial-zero.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688510443,
                value: 0
            },
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688515443,
                value: 0
            },
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688520443,
                value: 1.2
            },
        ]);
    });

    it('test-all-zero', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed-all-zero.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688510443,
                value: 0
            },
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688515443,
                value: 0
            },
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688520443,
                value: 0
            },
        ]);
    });

    it('test-intermediate-zero', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed-intermediate-zero.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688510443,
                value: 1.1
            },
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688515443,
                value: 0
            },
            {
                $source: "test-source",
                path: "environment.wind.speedApparent",
                time: 1606688520443,
                value: 1.2
            },
        ]);
    });

    it('test-multiple-source', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed-multiple-source.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                $source: "test-source1",
                path: "environment.wind.speedApparent",
                time: 1606688510443,
                value: 1.1
            },
            {
                $source: "test-source1",
                path: "environment.wind.speedApparent",
                time: 1606688515443,
                value: 1.1
            },
            {
                $source: "test-source1",
                path: "environment.wind.speedApparent",
                time: 1606688520443,
                value: 1.2
            },
            {
                $source: "test-source2",
                path: "environment.wind.speedApparent",
                time: 1606688510443,
                value: 1.2
            },
            {
                $source: "test-source2",
                path: "environment.wind.speedApparent",
                time: 1606688515443,
                value: 1.2
            },
            {
                $source: "test-source2",
                path: "environment.wind.speedApparent",
                time: 1606688520443,
                value: 1.3
            },
        ]);
    });
});

describe('from-batch-to-delta', function() {
    const from = batcher.from_batch_to_delta;

    it('test-basic-load', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                context: "vessels.urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
                updates: [{
                    $source: "test-source",
                    timestamp: new Date("2020-11-29T22:22:00.443Z"),
                    values: [{
                        path: "environment.wind.speedApparent",
                        value: 2.41
                    }]
                }]
            }
        ]);
    });

    it('test-unchanged-value', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed-unchanged.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                context: "vessels.urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
                updates: [{
                    $source: "test-source",
                    timestamp: new Date("2020-11-29T22:22:00.443Z"),
                    values: [{
                        path: "environment.wind.speedApparent",
                        value: 2.43
                    }]
                }]
            }
        ]);
    });

    it('test-unchanged-then-changed-value', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed-unchanged-then-changed.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                context: "vessels.urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
                updates: [{
                    $source: "test-source",
                    timestamp: new Date("2020-11-29T22:22:00.443Z"),
                    values: [{
                        path: "environment.wind.speedApparent",
                        value: 2.5
                    }]
                }]
            }
        ]);
    });

    it('test-initial-zero', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed-initial-zero.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                context: "vessels.urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
                updates: [{
                    $source: "test-source",
                    timestamp: new Date("2020-11-29T22:22:00.443Z"),
                    values: [{
                        path: "environment.wind.speedApparent",
                        value: 1.2
                    }]
                }]
            }
        ]);
    });

    it('test-all-zero', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed-all-zero.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                context: "vessels.urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
                updates: [{
                    $source: "test-source",
                    timestamp: new Date("2020-11-29T22:22:00.443Z"),
                    values: [{
                        path: "environment.wind.speedApparent",
                        value: 0
                    }]
                }]
            }
        ]);
    });

    it('test-intermediate-zero', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed-intermediate-zero.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                context: "vessels.urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
                updates: [{
                    $source: "test-source",
                    timestamp: new Date("2020-11-29T22:22:00.443Z"),
                    values: [{
                        path: "environment.wind.speedApparent",
                        value: 1.2
                    }]
                }]
            }
        ]);
    });

    it('test-multiple-paths', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed-direction.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                context: "vessels.urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
                updates: [{
                    $source: "test-source",
                    timestamp: new Date("2020-11-29T22:22:00.443Z"),
                    values: [{
                        path: "environment.wind.speedApparent",
                        value: 2.41
                    }]
                }]
            },
            {
                context: "vessels.urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
                updates: [{
                    $source: "test-source",
                    timestamp: new Date("2020-11-29T22:22:00.443Z"),
                    values: [{
                        path: "environment.wind.angleApparent",
                        value: -1.959385307179586
                    }]
                }]
            }
        ]);
    });

    it('test-multiple-source', function() {
        const test_data = load_data_from_disk('./test-from-batch-wind-speed-multiple-source.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                context: "vessels.urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
                updates: [{
                    $source: "test-source1",
                    timestamp: new Date("2020-11-29T22:22:00.443Z"),
                    values: [{
                        path: "environment.wind.speedApparent",
                        value: 1.2
                    }]
                }]
            },
            {
                context: "vessels.urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
                updates: [{
                    $source: "test-source2",
                    timestamp: new Date("2020-11-29T22:22:00.443Z"),
                    values: [{
                        path: "environment.wind.speedApparent",
                        value: 1.3
                    }]
                }]
            },
        ]);
    });

    it('test-complex-object', function() {
        const test_data = load_data_from_disk('./test-from-batch-position.json');

        const points = from(test_data);

        points.should.deep.equal([
            {
                context: "vessels.urn:mrn:signalk:uuid:635ed58a-540c-467a-a42b-b093056a5930",
                updates: [{
                    $source: "test-source",
                    timestamp: new Date("2020-11-29T22:22:00.443Z"),
                    values: [{
                        path: "navigation.position",
                        value: {
                            longitude: -122.1,
                            latitude: 47.7
                        }
                    }]
                }]
            }
        ]);
    });
});
