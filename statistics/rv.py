import warnings
import logging
import numpy as np
import scipy.stats
from pprint import pprint
logger = logging.getLogger(__name__)


def continuous_rvs():
    """Gets a list of all continuous random variables from scipy
    
    Returns:
        list: A list of sub-classes of scipy.stats.rv_continuous
    """
    rv_list = []
    for attr in dir(scipy.stats):
        f = getattr(scipy.stats, attr)
        if not callable(f):
            continue
        if issubclass(f.__class__, scipy.stats.rv_continuous):
            rv_list.append(f)
            continue
    return rv_list


def discrete_rvs():
    """Gets a list of all discrete random variables from scipy
    
    Returns:
        list: A list of sub-classes of scipy.stats.rv_discrete
    """
    rv_list = []
    for attr in dir(scipy.stats):
        f = getattr(scipy.stats, attr)
        if not callable(f):
            continue
        if issubclass(f.__class__, scipy.stats.rv_discrete):
            rv_list.append(f)
            continue
    return rv_list


def random_variables():
    """Gets a list of all random variables from scipy
    
    Returns:
        list: A list of sub-classes of scipy.stats.rv_continuous and scipy.stats.rv_discrete
    """
    rv_list = continuous_rvs()
    rv_list.extend(discrete_rvs())
    return rv_list

def fit_test_continuous_rv():
    """A list of default continuous random variables for fit_distribution()
    
    Returns:
        list: A list of continuous random variables
    """
    names = [
        "norm",
        "lognorm",
        "logistic",
        "laplace",
        "expon",
        "uniform",
        "levy",
        "cauchy",
    ]
    return [getattr(scipy.stats, name) for name in names]


def fit_distributions(samples, rv_list=None, filter_warnings='ignore'):
    """Fits a list of samples (numbers) to a list of random variable distribution.
    The "fitness" is determined by K-S test.
    # TODO: Use other goodness of fit tests.

    The data should not be tested on all random variable distributions.
    Because some random variables can produce the same distribution,
        when the parameters are selected carefully.
    Therefore, the numbers may fit multiple distributions in the same family.
    This function has an optional argument rv_list, 
        which should be a set of random variables from different families.
    Without specifying the rv_list parameter, 
        this function will use a small set of continuous variables returned by
        fit_test_continuous_rv()
    
    Args:
        samples ([type]): [description]
        rv_list ([type], optional): A list of candidate random variables. Defaults to None.
            If rv_list is None, the list returned by fit_test_continuous_rv() will be used.
        filter_warnings (str, optional): How to handle scipy warnings. Defaults to 'ignore'.
    
    Returns:
        list: A list of dictionaries, sorted by KS test D value.
            Each dictionary contains:
            name: The name of the fitted rv
            The best fit parameters
            The KS test D value
            The KS test p value
    """
    fits = []
    best_fit_rv = None
    best_d = 1
    best_parameters = None
    if rv_list == None:
        rv_list = fit_test_continuous_rv()
    total = len(rv_list)
    logger.debug("Fitting data to %d distributions." % total)
    counter = 1
    for rv in rv_list:
        logger.debug("Fitting data to %s...(%d/%d)" % (rv.name, counter, total))
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings(filter_warnings)
                if not hasattr(rv, "fit"):
                    continue
                parameters = rv.fit(samples)
                d, p = scipy.stats.kstest(samples, lambda x: rv.cdf(x, *parameters))
                fits.append({
                    "name": rv.name,
                    "parameters": parameters,
                    "D": d,
                    "p_value": p
                })
        except:
            logger.debug("Cannot fit to %s" % rv.name)
        counter += 1
    fits = sorted(fits, key=lambda i : i.get("D"))
    return fits