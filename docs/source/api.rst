.. -*- mode: rst -*-
.. vi: set ft=rst sts=4 ts=4 sw=4 et tw=79:

.. _chap_modref:

*************************
The NDI Package Reference
*************************

A quick overview of the ndi package's most important subpackages and modules. For more details on an item you can click its :code:`name`.


High-level user interface
=========================

DAQ Readers
------------------

.. currentmodule:: ndi.daqreaders
.. autosummary::
   :toctree: generated

   mock_daq_reader.MockReader

Databases and Tools
-------------------

.. currentmodule:: ndi.database
.. autosummary::
   :toctree: generated

   file_system.FileSystem
   sql.SQL
   query.Query

NDI Objects
------------------

.. currentmodule:: ndi
.. autosummary::
   :toctree: generated

   experiment.Experiment
   daq_system.DaqSystem
   probe.Probe
   channel.Channel
   epoch.Epoch

Test infrastructure
===================

.. currentmodule:: tests
.. autosummary::
   :toctree: generated

   tests.utils


.. raw:: html

   <h2>Indices and tables<h2>

* :ref:`genindex`
* :ref:`modindex`
