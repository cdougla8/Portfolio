# Predicting the next 49ers Play

*current accuracy: ~28%*

**Use a gradient boosted decision tree to try and predict if the 49ers will run one of the following plays:
- Run Left, Run Right, Run Center
- Pass Short Left, Pass Short Right, Pass Short Center
- Pass Long Left, Pass Long Right, Pass Long Center


#### Packages that need to be installed
- metaflow
- pandas
- numpy
- StringIO
- sklearn
- xgboost

#### To reproduce results:
1. ```python data_prep.py show```
2. ```python data_prep.py run```
3. ```jupyter-notebook Predictive_Analytics.ipynb```


## To Do
1. Add more features
2. Downsample
3. Better feature selection
4. Fine-tune model(s)
