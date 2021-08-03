If AUGUR Crashes on Ubuntu
===============

Augur is intended to run on a server operating system. On Ubuntu, there is a tracker service that occasionally causes Augur to inexplicably crash. When this happens, Augur does not collect data again until it is restarted.  The commands below disable this service, which prevents the collection free restart.  We are testing on Ubuntu's continuous release and will remove this documentation when the OS fixes are available. 

To Update Augur: 
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: json

	systemctl --user mask tracker-store.service tracker-miner-fs.service tracker-miner-rss.service tracker-extract.service tracker-miner-apps.service tracker-writeback.service
	
	tracker reset --hard




