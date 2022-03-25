#!/usr/bin/env bash

airflow db upgrade
airflow scheduler & exec airflow webserver
