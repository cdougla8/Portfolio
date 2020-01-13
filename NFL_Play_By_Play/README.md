# Predicting the next Ravens Play

*current accuracy: ~26%*

**Use a gradient boosted decision tree to try and predict if the Ravens will run one of the following plays:
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
1. ```python stats.py show```
2. ```python stats.py run```
3. ```jupyter-notebook stats.ipynb```


## To Do
1. Add more features
2. Downsample
3. Better feature selection
4. Fine-tune model(s)
