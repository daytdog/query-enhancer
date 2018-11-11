#!/usr/bin/python
#algorithms.py

import math

def muCalculator(lowerBound=3600.,upperBound=2592000.):
    """ Calculates the mu value for use in the sigmaCalculator
        and the gaussian curve calculator.
    Args:
        lowerBound (float)      - the time, in seconds, of the lower bound of the gaussian
        upperBound (float)      - the time, in seconds, of the upper bound of the gaussian
    Returns:
        mu (float)              - the time value of the center of the peak for the gaussian
    """
    return (upperBound - lowerBound)/2.


def sigmaCalculator(boundValue,boundsWeightConfig,peakWeightConfig,mu):
    """ Calculates the sigma value for use in the gaussian curve
        where the gaussian returns the boundsWeightConfig value for
        both the upper or lower bound time value.
    Args:
        boundValue (float)         - the time, in seconds, of either the lower or upper bound
        boundsWeightConfig (float) - the weight the gaussian will return for boundValue
        peakWeightConfig (float)   - the weight the gaussian will return for mu
        mu (float)                 - the time value of the center of the peak for the gaussian
    Returns:
        sig (float)                - the value of sigma that will cause the gaussian to return
                                     boundsWeightConfig at both the upper and lower bound
    Equations:
        sigma = (x-mu)/sqrt(-2*ln((y-1)/(z-1)))
            where x = boundValue, mu = mu, y = boundsWeightConfig, and z = peakWeightConfig
            and ln is the natural logarithm
    """
    return abs((boundValue-mu)/math.sqrt(-2.*math.log((boundsWeightConfig-1.)/(peakWeightConfig-1.))))


def gaussian(time,mu,sigma,peakWeightConfig=1.4):
    """ Returns the value of the gaussian curve for a given time
    Args:
        time (float)                - the time, in seconds, that a gaussian weight will be
                                      calculated for
        mu (float)                  - the time value of the center of the peak for the gaussian
        sigma (float)               - the value of sigma that will cause the gaussian to return
                                      boundsWeightConfig at both the upper and lower bound
        peakWeightConfig (float)    - the weight the gaussian will return for mu
    Returns:
        timeWeight (float)          - the weight the gaussian returns for a given input time
    Equations:
        mu
            see the muCalculator method
        sigma
            see the sigmaCalculator method
        timeWeight = 1 + (z-1)*exp(-0.5*((x-mu)/sigma)^2)
            where x = time, mu = mu, and z = peakWeightConfig
            and exp is the exponential e^x where x is the input of the exp function
    Notes:
        when running this algorithm on a set of data, it's better to calculate sigma
        and mu first, and pass them as arguments to this algorithm.

        mu = muCalculator(lowerBound,upperBound)
        sigma = sigmaCalculator(upperBound,boundsWeightConfig,peakWeightConfig,mu)
    """
    return 1. + (peakWeightConfig-1.)*math.exp(-0.5*((time-mu)/sigma)**2)

def priorityLevelWeighting(sortedPriority,priorityLevelsMaxWeight=1.5,numPriorityLevels=4):
    """ Returns the weight a sortedPriority value has for a grouping with x levels
        of priority.
    Args:
        sortedPriority (int):            - the sorted value of a ticket's priority level,
                                           the lower the value, the higher the priority
        priorityLevelsMaxWeight (float): - the value of the weight that the message will
                                           have if its priority level is the highest
                                           priority possible (i.e. sortedPriority=0)
        numPriorityLevels (int):         - the number of levels of priority that the group
                                           the message belongs to has
    Returns:
        priorityWeight (float):          - the value of the weight of the message's priority
                                           level
    Equations:
        priorityWeight = w - (((w-1)/(z-1)) * x)
            where x = sortedPriority, w = priorityLevelsMaxWeight, and z = numPriorityLevels
    """

    return priorityLevelsMaxWeight - (((priorityLevelsMaxWeight-1.)/(numPriorityLevels-1.))*sortedPriority)

def inverseWeighting(weight_list):
    """ Calculates the total relevancy score a document will have
        Rather than multiplying the weights together, this will do the inverse, where applying
        additional weights will cause the score to approach the value of 1, or 100% additional
        weighting. With this method, however, the total score will always be >=0 and <1.
    Args:
        weight_list (list)          - a list of weights to be concatenated into the relevancy score
    Returns:
        weighted_score (float)      - the result of inverse concatenation of the weights

    Equations:
        C = (2 - W1) * (2 - W2) * ... * (2 - Wn)
            where W1,W2,...,Wn are the weights
        weighted_score = 1 - C
    """
    C = 1.
    for weight in weight_list:
        C *= (2. - weight)
    return 1. - C

def logisticsGrowth(numWorkNotes,upperNumWorkNotesBound=20,minNumWorkNotes=2,upperNumBoundWeight=1.4):

    return 2./(1.+math.exp(((math.log((2.-upperNumBoundWeight)/upperNumBoundWeight)/(upperNumWorkNotesBound-minNumWorkNotes))*(numWorkNotes-minNumWorkNotes))))

def logisticsGrowth2(numTokens,upperNumTokensBound=150.,minNumTokens=10.,upperBoundWeight=1.4):

    return upperBoundWeight / (1.+(upperBoundWeight-1)*math.exp((1./(minNumTokens-upperNumTokensBound))*(math.log(upperBoundWeight-1.)-math.log((3.*(upperBoundWeight-1.)**2)+4.*(upperBoundWeight-1.)))*(minNumTokens-numTokens)))

def logGrowth(inputNum,minNum=2,upperNumBound=20,upperBoundWeight=1.4):

    return 1. + (math.log(inputNum-(minNum-1))*(upperBoundWeight-1))/(math.log(upperNumBound))

def exponential(inputNum,minNum=0,upperNumBound=4,upperBoundWeight=1.4):

    return math.exp((math.log(upperBoundWeight)/(upperNumBound-minNum-1))*(inputNum-minNum))

def cosineGrowth(sortedPriority,numPriorityLevels=4,maxPriorityWeight=1.5):

    return ((maxPriorityWeight-1.0)/2.)*(math.cos(math.pi*(((sortedPriority)/(numPriorityLevels-1.))-1.))+1.)+1.

#For Log-Normal Distribution
def normMu(lowerTimeBound=7200,upperTimeBound=28800):

    return (upperTimeBound - lowerTimeBound)/2. + lowerTimeBound

def normSigmaCalculator(mu,lowerTimeBound=7200,lowerTimeBoundWeight=0.1,peakWeight=0.4):

     return abs((lowerTimeBound-mu)/math.sqrt(-2.*math.log((lowerTimeBoundWeight)/(peakWeight))))

def normGaussian(time,mu,normSigma,peakWeight=0.4):

    return (peakWeight)*math.exp(-0.5*((time-mu)/normSigma)**2)

def normGaussianWrapper(time,lowerTimeBound=7200,upperTimeBound=28800,lowerTimeBoundWeight=0.1,peakWeight=0.4):
    mu = normMu(lowerTimeBound,upperTimeBound)
    normSigma = normSigmaCalculator(mu,lowerTimeBound,lowerTimeBoundWeight,peakWeight)
    normGaussianValue = normGaussian(time,mu,normSigma,peakWeight)
    return normGaussianValue




def logNormSigmaCalculator(peakWeightTime,lowerTimeBound=7200,lowerTimeBoundWeight=1.1,peakWeight=1.4):

    z = peakWeight*peakWeightTime

    logNormSigma = (math.log(lowerTimeBound) - math.log(peakWeightTime))/math.sqrt(-2.*math.log((lowerTimeBoundWeight*lowerTimeBound)/z))

    return z,abs(logNormSigma)

def logNormDistribution(time,logNormSigma,z,peakWeightTime):

    return (z/time)*math.exp((-0.5)*((math.log(time)-math.log(peakWeightTime))/logNormSigma)**2.)

def logNormWrapper(time,lowerTimeBound=7200,upperTimeBound=28800,lowerTimeBoundWeight=0.1,peakWeight=0.4):

    peakWeightTime = normMu(lowerTimeBound,upperTimeBound)
    z,logNormSigma = logNormSigmaCalculator(peakWeightTime,lowerTimeBound,lowerTimeBoundWeight,peakWeight)

    logNormValue = logNormDistribution(time,logNormSigma,z,peakWeightTime)
    return logNormValue,logNormSigma

#bring them both together
def fullWrapper(time,lowerTimeBound=7200,upperTimeBound=28800,lowerTimeBoundWeight=0.1,peakWeight=0.4):
    normGaussianValue = normGaussianWrapper(time,lowerTimeBound,upperTimeBound,lowerTimeBoundWeight,peakWeight)
    logNormValue = logNormWrapper(time,lowerTimeBound,upperTimeBound,lowerTimeBoundWeight,peakWeight)+1.
    #return normGaussianValue
    #return logNormValue - normGaussianValue
    #if (time >= lowerTimeBound) and (time <= upperTimeBound):
    #    return logNormValue - normGaussianValue
    #else:
    #    return logNormValue
