Updating Augur
===============

Augur goes through regular releases. Usually every 2 weeks. We are careful to ensure backward compatibility through version 0.17.0, though a migration script will likely be required when we move to version 0.2.0.  This migration script will make updates to the schema, and the execution of data validation scripts to ensure your schema and data integrity remain in tact. 

To Update Augur: 
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: json

	cd $AUGUR_HOME
	git pull
	make rebuild-dev
