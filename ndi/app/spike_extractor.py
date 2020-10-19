from .app import NDI_App

class SpikeExtractor(NDI_App):
    name =  'ndi_app_spike_extractor'
    def __init__(self, channel):
        self.channel = channel

    def extract(self, *parameters):
        pass
