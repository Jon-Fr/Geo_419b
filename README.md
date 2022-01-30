# Geo_419b
This program is intended for the automatic download of elevation data and orthophotos from [Geoportal-Th.de][1].

## Installation
The following section describes the installation of the program. Firstly, in case [Anaconda][2] is used and secondly, if it is not.

#### With Anaconda
If you use anaconda you can use the enviroment.yml file to set up an environment in which the script can be used. 

Start by cloning this repository to your local system.

Then enter the following two commands in the Anaconda Prompt.
```sh
cd "Path of the repository folder"
conda env create -f environment.yml
```
If you want to change the name of the environment you have to do it in the .yml file.

Additional information about setting up environments using .yml files can be found [here][3].

After you have set up the environment like this, it should be possible to use the script within it without any further action.
```
#### Without Anaconda
If you do not use Anaconda, you will have to manually install the packages that are imported at the beginning of the script.
If you are not sure which version of a package to install and which Python version is appropriate, you can check the .yml file.


[1]: https://www.geoportal-th.de/de-de/
[2]: https://www.anaconda.com/
[3]: https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file

