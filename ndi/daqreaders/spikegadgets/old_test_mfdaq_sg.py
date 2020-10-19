from ndi.schema.ChannelType import ChannelType
from ndi.daqsystem.mfdaq import DaqSystemMultiFunction
from ndi.daqsystem.daqreaders.spikegadgets.mfdaq_sg import SpikeGadgetChannel, DaqReaderMultiFunctionSg


def test_spikegadget_channel_d_in():
    sg_ch = SpikeGadgetChannel('Din2')
    assert sg_ch.type == ChannelType.digital_in
    assert sg_ch.name == 'di2'
    assert sg_ch.number == 2


def test_spikegadget_channel_d_out():
    sg_ch = SpikeGadgetChannel('Dout9')
    assert sg_ch.type == ChannelType.digital_out
    assert sg_ch.name == 'do9'
    assert sg_ch.number == 9


def test_spikegadget_channel_aux_in():
    sg_ch = SpikeGadgetChannel('Ain5')
    assert sg_ch.type == ChannelType.auxiliary
    assert sg_ch.name == 'axn5'
    assert sg_ch.number == 5


def test_spikegadget_channel_aux_out():
    sg_ch = SpikeGadgetChannel('Aout5')
    assert sg_ch.type == ChannelType.auxiliary
    assert sg_ch.name == 'axo5'
    assert sg_ch.number == 5


def test_spikegadget_channel_mcu():
    sg_ch = SpikeGadgetChannel('MCU_Din1')
    assert sg_ch.type == ChannelType.digital_in
    assert sg_ch.name == 'di33'
    assert sg_ch.number == 33


def test_read_req():
    reader = DaqReaderMultiFunctionSg(
        'tests/data/daqreaders/spikegadgets/CS31_20170201_OdorPlace1short.rec')
    daq_system = DaqSystemMultiFunction('test_daq', reader)

    assert daq_system
