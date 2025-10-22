# Databricks notebook source
# Disable writing __pycache__ (important for Databricks workspace paths)
import sys
sys.dont_write_bytecode = True

# Run pytest
!pytest -v /Workspace/Users/siddharthsinha.28@gmail.com/Pei_Assisgnment/tests/gold/test_agg.py
