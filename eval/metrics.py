import math
import util
#
# metric class to hold raw metric value and supporting data
#
class Metric:
    def __init__(self, name, defaultValue):
        self.name = name
        self.lowerBound = None
        self.upperBound = None
        self.defaultValue = defaultValue
        self.value = 0
    def float(self,normalized = False):
        if normalized:
            if (self.lowerBound == None) or (self.upperBound == None) or (self.lowerBound == self.upperBound):
                return self.defaultValue
            else:
                return self.value / (self.upperBound - self.lowerBound)
        else:
            return self.value
    def string(self,normalized = False):
        v = self.float(normalized)
        return "%f"%v

#
# INDIVIDUAL METRICS
#

#
# relevance
#
class Relevance(Metric):
    def __init__(self, target, umType, p, u, relevanceLevels, n):
        super().__init__("relevance",1.0)
        self.target = target
        #
        # upper bound: the run reproduces the target exposure
        #
        self.upperBound = util.l2(target, False)
        #
        # lower bound: static ranking in reverse relevance order or 0 if retrieval
        #
        self.lowerBound = 0.0
        if (n != math.inf):
            pp = p if (umType == "rbp") else p * u
            dist = []
            for d,e in target.items():
                dist.append(e)
            dist.sort()
            for i in range(len(dist)):
                self.lowerBound = self.lowerBound + pow(pp,i)*dist[i]
        if len(relevanceLevels) <= 1:
            self.upperBound = 0.0
            self.lowerBound = 0.0
            
    def compute(self,run):
        self.value = util.dot(self.target,run)

#
# disparity
#
class Disparity(Metric):
    def __init__(self, target, umType, p, u, relevanceLevels, n):
        super().__init__("disparity",0.0)
        #
        # upper bound: static ranking
        #
        self.upperBound = util.geometricSeries(p*p,n)
        #
        # lower bound: uniform random
        #
        self.lowerBound = 0.0
        if (n != math.inf):
            pp = p if (umType == "rbp") else p * u
            self.lowerBound = pow( util.geometricSeries(pp,n), 2 ) / n
    def compute(self,run):
        self.value = util.l2(run,False)

#
# difference
#
class Difference(Metric):
    def __init__(self, target, umType, p, u, relevanceLevels, n):
        super().__init__("difference",0.0)
        self.target = target
        #
        # lower bound: run exposure reproduces target exposure
        #
        self.lowerBound = 0.0
        #
        # upper bound
        #
        # retrieval setting (n == math.inf)
        #
        # assume that all of the documents in target exposure with values > 0 are at 
        # the bottom of the ranking.  the upper bound, then, is decomposed into two 
        # parts.  we assume that the exposure at the end of the ranking is effectively
        # zero and the quantity is the exposure "lost" from the relevant documents,
        #
        # \sum_{i=0}^{len(target)} target(i)*target(i)
        #
        # and the second is the exposure "gained" for the nonrelevant documents.  we 
        # assume that the corpus is of infinite size and that the relevant documents
        # are all at the end.  we're technically double counting the end but the
        # contribution to the geometric series is so small it should not matter.  
        #
        # \sum_{i=0} p^i * p^i 
        #
        # reranking setting (n != math.inf)
        #
        # assume the worst exposure is a static ranking in reverse order of relevance
        #
        pp = p if (umType == "rbp") else p * u        
        ub = 0.0
        if (n == math.inf):
            #
            # retrieval condition
            #
        
            # contribution lost from relevant documents
            for d,e in target.items():
                ub += e*e
            # contribution gained from nonrelevant documents
            ub = ub + util.geometricSeries(pp,n)
        else:
            #
            # reranking condition
            #
            # construct the sorted target exposure
            target_vector = []
            for d,e in target.items():
                target_vector.append(e)
            target_vector.sort()
            for i in range(len(target_vector)):
                diff = pow(pp,i) - target_vector[i]
                ub += diff*diff
        self.upperBound = ub
    def compute(self,run):
        self.value = util.distance(self.target, run, False)
#
# GROUP METRICS
#

#
# group relevance
#
class GroupRelevance(Metric):
    def __init__(self, target, umType, p, u, r, k):
        super().__init__("relevance",1.0)
        self.target = target
        #
        # upper bound: assume the all groups get all of the exposure (loose)
        #
        attention_mass = util.geometricSeries(p,r)
        ub = 0.0
        for v in target.values():
            ub += v * ub
        self.upperBound = ub
        
        #
        # lower bound: assume a non-relevant group gets all of the exposure
        #
        self.lowerBound = 0.0
            
    def compute(self,run):
        self.value = util.dot(self.target,run)

#
# group disparity
#
class GroupDisparity(Metric):
    def __init__(self, target, umType, p, u, r, k):
        super().__init__("disparity",0.0)
        if (k==1):
            self.lowerBound = 0
            self.upperBound = 0
        else:
            #
            # upper bound: assume all groups get all of the exposure
            #
            e = util.geometricSeries(p,r) 
            self.upperBound = e * e * k
            #
            # lower bound: assume all groups get equal exposure
            #
            lb = 0.0
            if (r != math.inf):
                pp = p if (umType == "rbp") else p * u        
                exposure = util.geometricSeries(pp,r)
                lb = k * exposure * exposure
            self.lowerBound = lb
    
    def compute(self,run):
        self.value = util.l2(run,False)

#
# group difference
#
class GroupDifference(Metric):
    def __init__(self, target, umType, p, u, r, k):
        super().__init__("difference",0.0)
        self.target = target
        #
        # upper bound: assume the least relevant group gets all of the exposure
        #
        exposure_mass = util.geometricSeries(p,r)
        #
        # case 1: no exposure to any relevant group
        #
        norm = 0.0
        for d,e in target.items():
            norm += e*e
        #
        # case 2: max exposure to all groups
        #
        retval = 0.0
        dist = []
        diff = 0.0
        for d,e in target.items():
            dist.append(e)
            diff += (exposure_mass-e)*(exposure_mass-e)
        self.upperBound = norm if (norm > diff) else diff
        #
        # lower bound: 
        #
        self.lowerBound = 0.0
    
    def compute(self,run):
        self.value = util.distance(self.target, run, False)
