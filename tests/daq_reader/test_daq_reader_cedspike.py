from ndi.daqreaders.cedspike2 import CEDSpike2
import pytest


class TestCEDSpike2:
    @pytest.mark.parametrize(
        'test_file, channel_number, expected_samplerate',
        [
            ('./tests/data/daqreaders/cedspike2/example1.smr', 21, 1 / .00009),
            ('./tests/data/daqreaders/cedspike2/example2.smr', 21, 1 / .00009),
            ('./tests/data/daqreaders/cedspike2/example3.smr', 21, 1 / .00009),
            ('./tests/data/daqreaders/cedspike2/DemoData.smr', 1, 1 / .005),
            ('./tests/data/daqreaders/cedspike2/DemoData.smr', 2, 1 / .004),
            ('./tests/data/daqreaders/cedspike2/DemoData.smr', 3, 1 / .01),
        ]
    )
    def test_samplerate(self, test_file, channel_number, expected_samplerate):
        daq_reader = CEDSpike2(test_file)
        assert daq_reader.samplerate(channel_number) == expected_samplerate

    @pytest.mark.parametrize(
        'test_file, channel_number,  start_index, end_index, expected_length, expected_values_slice',
        [
            ('./tests/data/daqreaders/cedspike2/example1.smr', 21, 0, None, 5569702, [-0.9647, -0.9592, -0.9644, -0.9601, -0.9644]),
            ('./tests/data/daqreaders/cedspike2/example2.smr', 21, 500, 505, 5, [-0.7330, -0.7352, -0.7346, -0.7318, -0.7352]),
            ('./tests/data/daqreaders/cedspike2/example3.smr', 21, 1234, 2345, 1111, [-0.8685, -0.8685, -0.8640, -0.8728, -0.8621]),
            ('./tests/data/daqreaders/cedspike2/DemoData.smr', 1, 86, 101, 15, [-0.0391, -0.0391, 0.1927, 0.5370, 0.9959]),
            ('./tests/data/daqreaders/cedspike2/DemoData.smr', 2, 44, 57, 13, [112.7167, 116.3635, 110.0616, 105.4535, 105.4993]),
            ('./tests/data/daqreaders/cedspike2/DemoData.smr', 3, 42, 84, 42, [0.1591, 0.2080, 0.2129, 0.2226, 0.2422]),
        ]
    )
    def test_readchannel(self, test_file, channel_number, start_index, end_index, expected_length, expected_values_slice):
        daq_reader = CEDSpike2(test_file)
        data = daq_reader.readchannel(channel_number, start_index, end_index)
        assert len(data) == expected_length
        for i, item in enumerate(expected_values_slice):
            assert item == round(float(data[i]), 4)

    @pytest.mark.parametrize(
        'test_file, channel_number, start_time, end_time, expected_length, expected_values_slice',
        [
            ('./tests/data/daqreaders/cedspike2/example1.smr', 22, 0, None, 46, [9.4653, 20.2397, 31.0161, 41.7925, 52.5689]),
            ('./tests/data/daqreaders/cedspike2/example2.smr', 22, 200, 400, 18, [210.2610, 221.0374, 231.8137, 242.5911, 253.3675]),
            ('./tests/data/daqreaders/cedspike2/example3.smr', 22, 350, 450, 5, [357.4348, 377.4465, 397.4573, 417.4690, 437.4807]),
            ('./tests/data/daqreaders/cedspike2/DemoData.smr', 4, 15, None, 51, [15.6375, 16.5390, 17.4485, 18.3618, 19.2071]),
            ('./tests/data/daqreaders/cedspike2/DemoData.smr', 5, 30, 50, 50, [33.0790, 33.1012, 33.1051, 33.1210, 33.1420]),
            ('./tests/data/daqreaders/cedspike2/DemoData.smr', 6, 0, 15, 5, [0.0013, 3.2781, 6.5549, 9.8317, 13.1085]),
        ]
    )
    def test_readevents(self, test_file, channel_number, start_time, end_time, expected_length, expected_values_slice):
        daq_reader = CEDSpike2(test_file)
        events = daq_reader.readevents(channel_number, start_time, end_time)
        assert len(events) == expected_length
        for i, item in enumerate(expected_values_slice):
            assert item == round(float(events[i]), 4)
