# Airflow-Practice

#### By [Reed Carter](https://github.com/Reed-Carter)

#### This repo is a code review to demonstrate a basic understanding of Airflow. It uses airflow to orchestrate events in order to complete simple dummy tasks 

<br>

## Technologies Used

* Airflow
* Pandas
* Git
* Markdown
  
</br>

## Description

This repo demonstrates an automated process using Arflow. A DAG was created to represent the toy tasks that were created to run which can be seen below. 

[<img src="images/Code-Review-DAG.png">](images/Code-Review-DAG.png)


## Setup/Installation Requirements

* Go to https://github.com/Reed-Carter/Airflow-practice.git to find the specific repository for this website.

* Then clone the repository by inputting: 
  ```bash
  git clone https://github.com/Reed-Carter/Airflow-practice.git
  ```
* Go into the new directory:
  ```bash
  cd Airflow-Practice
  ```
* Once in the directory run the setup file in order to create a virtual environment, install airflow on the virtual environment, and install all required dependencies using the command:
  ```bash
  ./setup.sh
  ```
* Since the required directories are already in place, set your user id as the $AIRFLOW_UID in a .env file:
  ```bash
  echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
  ```
* Now airflow can be initializaed with:
  ```bash
  docker-compose up airflow-init
  ```
* if you run `docker ps -a` you should see running containers
* Now the remaining containers can be started using:
  ```bash
  docker-compose up
  ```
* Leave this running, and open a new terminal tab to use the command line.
* Navigate to http://0.0.0.0:8080/home in your browser and loging as the default user `airflow` with the password also `airflow`. 

</br>

## Known Bugs

* No known bugs

<br>

## License

MIT License

Copyright (c) 2022 Ruben Giosa, Reed Carter, Chloe (Yen Chi) Le

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
