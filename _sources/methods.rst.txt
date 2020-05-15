Methods
=======

Two branches of statistical learning tools are widely used today:

Unsupervised Learning:
  An unsupervised learning method takes inbound unlabeled data and extracts or discovers classificiations and labels in the data.

Supervised Learning
  In a supervised learning context, inbound data are labeled and an analyst builds features that they likely believe will hold predictive power.  They might use an understanding of the physics or mechanisms of a system to decide on those features.

This study explored the use of two supervised learning methods:

Logistic Regression:
  
  A binary classification algorithm which assigns a probability that some set of features could be labeled in a certain way.

.. toctree::
  :maxdepth: 1
  :caption: notebooks

  notebooks/1-ttu-logistic-weather

Statistical Learning Process
----------------------------

1.  A class of variables that we want to predict and use to predict are labeled in an existing dataset.  The predictive variables are called "estimators"
2.  The data are partitioned into two parts - a "training" set which is used to develop the model, and a "test" set which is used to validate the model.
3.  A statistical model is "fit" to the training data, providing a statistical function which could take in new observations and predict the originals labels.
4.  The model is used to predict the "test" dataset, and the true label values are compared against the predicted label values.
5.  Performance metrics of the model are calculated against the test data.
6.  If the model is robust, it could in principle be deployed (either in a programmatic or manual environment) to ingest a future data stream where events are not known a-priori.


The choice of a statistical model

Tooling
-------

The process above was implemented in scikit-learn :cite:`scikit-learn`, a popular machine learning library in the python ecosystem.  Other tools of note include:

`pycaret <https://pycaret.org/>`_
  PyCaret is a machine learning framework for quickly producing un-optimized a


.. toctree::
  :maxdepth: 1
  :caption: notebooks

  notebooks/3-ttu-train_models