import math
import util
from metrics import GroupDisparity
from metrics import GroupRelevance
from metrics import GroupDifference

def exposure(exposures, did2gids, qrels, complete):
    groupExposures = {}
    for did,gids in did2gids.items():
        if (complete) or ((did in qrels) and (qrels[did] > 0)):
            for gid in gids:
                if not(gid in groupExposures):
                    groupExposures[gid] = 0.0
                if did in exposures:
                    groupExposures[gid] += exposures[did]
    return groupExposures

def metrics(target, umType, p, u, n, r):
    k = len(target)
    disparity = GroupDisparity(target, umType, p, u, r, k)
    relevance = GroupRelevance(target, umType, p, u, r, k)
    difference = GroupDifference(target, umType, p, u, r, k)
    return disparity, relevance, difference
