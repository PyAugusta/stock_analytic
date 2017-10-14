from analytic import assets
from analytic.assets import utils
import nltk.classify.util
from nltk.classify import NaiveBayesClassifier
from nltk.corpus import movie_reviews
import os


def word_feats(words):
    return dict([(word, True) for word in words])

def train_test():
    negids = movie_reviews.fileids('neg')
    posids = movie_reviews.fileids('pos')

    negfeats = [(word_feats(movie_reviews.words(fileids=[f])), 'neg') for f in negids]
    posfeats = [(word_feats(movie_reviews.words(fileids=[f])), 'pos') for f in posids]

    negcutoff = len(negfeats) * 3 // 4
    poscutoff = len(posfeats) * 3 // 4

    trainfeats = negfeats[:negcutoff] + posfeats[:poscutoff]
    testfeats = negfeats[negcutoff:] + posfeats[poscutoff:]
    print('train on %d instances, test on %d instances' % (len(trainfeats), len(testfeats)))

    classifier = NaiveBayesClassifier.train(trainfeats)
    print('accuracy:', nltk.classify.util.accuracy(classifier, testfeats))
    classifier.show_most_informative_features()

    utils.pickle_it(classifier, assets.text_classifier_pickle)
    return classifier


def get_classifier(refresh=False):
    if refresh or not os.path.exists(assets.text_classifier_pickle):
        classifier = train_test()
    else:
        classifier = utils.unpickle_it(assets.text_classifier_pickle)
    return classifier
