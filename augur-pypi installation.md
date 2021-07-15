  To install the augur-pypi refer to the following steps:

1)Clone the augur-pypi2 branch into your local machine:

    git clone https://github.com/chaoss/augur.git augur-pypi2
    

2)After cloning, go the the cloned directory in your local machine


3)Create a new virtual environment:

    python3 -m venv $HOME/.virtualenvs/augur_env
  
4)Activate the virtual environment:

    source $HOME/.virtualenvs/augur_env/bin/activate
  
5)Now you're all set to install augur-pypi package, to install the pypi package use the following command:

    python3 -m pip install augur-0.16.2-py3-none-any.whl
