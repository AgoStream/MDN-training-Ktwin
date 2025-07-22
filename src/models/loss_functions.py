import tensorflow as tf
from tensorflow_probability import distributions as tfd


def mdn_cost(mu, sigma, y):
    dist = tfd.distributions.Normal(loc=mu, scale=sigma)
    return tf.reduce_mean(-dist.log_prob(y))


def gnll_loss(y, alpha, mu, sigma):
    """ Computes the mean negative log-likelihood loss of y given the mixture parameters.
    """
        
    gm = tfd.MixtureSameFamily(
        mixture_distribution=tfd.Categorical(probs=alpha),
        components_distribution=tfd.Normal(
            loc=mu,       
            scale=sigma))
    
    log_likelihood = gm.log_prob(tf.transpose(y)) # Evaluate log-probability of y
    
    return -tf.reduce_mean(log_likelihood, axis=-1)


# Cost for the multiple components MDN using GAMMA distribution
def gammanll_loss(y, alpha, concentration, rate):
    """ Computes the mean negative log-likelihood loss of y given the mixture parameters. """
        
    gm = tfd.MixtureSameFamily(
        mixture_distribution=tfd.Categorical(probs=alpha),
        components_distribution=tfd.Gamma(
            concentration=concentration,       
            rate=rate))
    
    log_likelihood = gm.log_prob(tf.clip_by_value(tf.transpose(y), 1e-8, 100)) # Evaluate log-probability of y np.clip(tf.transpose(y), 1e-8, 100.)
    
    return -tf.reduce_mean(log_likelihood, axis=-1)


# Cost for the multiple components MDN using Weibull distribution
def weibullnll_loss(y, alpha, concentration, scale):
    """ Computes the mean negative log-likelihood loss of y given the mixture parameters.
    """
        
    gm = tfd.MixtureSameFamily(
        mixture_distribution=tfd.Categorical(probs=alpha),
        components_distribution=tfd.Weibull(
            concentration=concentration,       
            scale=scale))
    
    log_likelihood = gm.log_prob(tf.clip_by_value(tf.transpose(y), 1e-8, 100)) # Evaluate log-probability of y np.clip(tf.transpose(y), 1e-8, 100.)
    
    return -tf.reduce_mean(log_likelihood, axis=-1)
