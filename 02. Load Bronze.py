# Databricks notebook source
dbutils.widgets.text(name= "env", defaultValue="", label="Environment(LowerCase)")
env = dbutils.widgets.get("env")
