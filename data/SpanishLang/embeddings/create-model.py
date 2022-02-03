import fasttext
import sys
import os

filename = sys.argv[1]
outname = filename.replace(".txt", ".bin")
if not os.path.isfile(outname):
    model = fasttext.train_unsupervised(filename, model="skipgram", dim=300)
    model.save_model(outname)
