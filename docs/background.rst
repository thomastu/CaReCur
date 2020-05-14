Motivation
==========

This is part of an independent study to apply statistical approaches to describing renewable curtailment events.  
Primarily, we are motivated by the following questions:

- What are explanatory or predictive variables that can predict economic solar curtailment in the California grid?
- What are the policies and mechanisms that are used to deploy economic curtailment?
- Given a day ahead weather forecast, can we predict the net load (total load - renewable intermittent generation)?
  - With a week ahead forecast?
-  Can we calculate or measure the economic value from existing curtailment practices?

Background
----------

As intermittent sources of generation continue to grow, overcapacity may result in events where generation may exceed available demand.  Balancing authorities must decide how to ramp down total generation to keep the grid in balance.  Currently the CAISO approaches oversupply primarly through economic curtailment :cite:`CaliforniaISO2017`.  Other approaches to managing supply include expanded energy storage, demand response programs, :term:`TOU` rates, reductions in minimum generation, expanding the western :term:`EIM`, increasing flexible capacity, integrating more EVs and improving regional coordination :cite:`CaliforniaISO2017a`.  The goal of this study is to better inform and understand exogenous variables that contribute to oversupply.

.. todo::

  Flesh out a few additional motivations:

  - Growth of installed solar capacity from 2015+ vs mean electricity demand over analysis months (Feb to May)
  - Use cases for curtailed energy such as dispatching short-term DR
    - Sample plots showing potential for system benefits using even a low-probability model.