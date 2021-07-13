API Keys and AUGUR
~~~~~~~~~~~~~~~~~~

Contents
========
1. :ref:`What are keys?`

2. :ref:`Where do I get keys?`

3. :ref:`What does Augur use API keys for?`

4. :ref:`Where does Augur need API keys in the build process?`


What are keys?
***************

    To create a smoothly running interface between sofware applications. API implementations are used so that information 
    can be exchanged to create many software solutions. To accomplish this end, API keys are used. This helps to reduce the
    difficulty of creating derivative software and keep the parent software secure.

    Here, an API key is a unique identifier and a secure token to provide access to an API. An API key is usually generated with
    a particular set of privilegdes for the entity using it. API keys are mainly used for software applications derivative of the 
    parent application in order to use a feature or access data belonging to the parent.

    Examples of software solutions that use API keys are twitter bots, discord bots and AUGUR.

Where do I get keys?
********************

    API keys are provided by the entity that is providing the API. Typically, this provider gives instructions for developers through 
    a 'developer portal' or similar access point to generate a key. The key gives specified access to specified aspects of the application
    providing the API.
    
    Specifically, installation requires API keys from both Github and Gitlab.

    For Github, special API privileges will need to be enabled. Namely, repo and all read privilegdes except 'enterprise.' The API keygen
    page can be found `here <https://github.com/settings/tokens>`_; simply check the required elements and save the generated key.

    For Gitlab, the process is similar. `This <https://github.com/settings/tokens>`_ webpage will let you select the required privilegdes along
    with the date you plan to no longer need the API key. Make sure api, read_user, read_api, read_repository, write_repository are all selected
    before you save the key.

What does Augur use API keys for?
*********************************
    AUGUR uses APIs from Github and Gitlab in order to make requests for repository information.

    AUGUR also generates an API key for use in applications that want to use AUGUR functionality via an API.

Where does Augur need API keys in the build process?
****************************************************

    The standard installation should prompt you to input the required API keys. No config file is neccessary as it will be generated for you.