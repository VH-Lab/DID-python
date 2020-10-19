from ndi.epoch_probe_map.vhintan_channelgrouping import read_device_string, VHIntanChannelGrouping
from ndi import FileNavigator, Epoch, Probe, Channel
from ndi.daqreaders import CEDSpike2
import pytest


class TestEpochProbeMap:
    @pytest.mark.parametrize(
        'device_string, expected_output',
        [
            (
                'mySpike2:ai3-5,7,9-11',
                {
                    'device_name': 'mySpike2',
                    'channel_type': 'analog_in',
                    'channel_list': [3, 4, 5, 7, 9, 10, 11]
                }
            ),
            (
                'dev1:e22,24,30-33',
                {
                    'device_name': 'dev1',
                    'channel_type': 'event',
                    'channel_list': [22, 24, 30, 31, 32, 33]
                }
            ),
            (
                'name:t3-4,6,8-10',
                {
                    'device_name': 'name',
                    'channel_type': 'time',
                    'channel_list': [3, 4, 6, 8, 9, 10]
                }
            )
        ]
    )
    def test_read_device_string(self, device_string, expected_output):
        # Test that function gives expected output
        assert read_device_string(device_string) == expected_output

    def test_vh_intan_channel_grouping(self):
        # Instantiate FileNavigator
        fn = FileNavigator(['.*\.smr$', '.*\.epochmetadata$'], '.*\.epochmetadata$')
        # Acquire epoch_sets from FileNavigator
        epoch_sets = fn.get_epoch_set('./tests/data/intracell_example')
        # Instantiate epochprobemap_reader object
        epochprobemap_reader = VHIntanChannelGrouping(CEDSpike2, epoch_sets, 'test_exp_id')

        epochs, probes, channels = epochprobemap_reader.get_epochs_probes_channels()

        # Test that there are 3 epochs in intracell_example
        assert len(epochs) == 3
        for epoch in epochs:
            # Test that each item in list is Epoch object
            assert type(epoch) == Epoch

        # Test that there are 2 probes in intracell_example
        assert len(probes) == 2
        for probe in probes:
            # Test that each item in list is Probe object
            assert type(probe) == Probe
        
        # Test that there are 3 channels in intracell_example
        assert len(channels) == 3
        for channel in channels:
            # Test that each item in list is Channel object
            assert type(channel) == Channel
