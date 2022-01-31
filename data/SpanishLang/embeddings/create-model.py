import fasttext
import sys

filename = sys.argv[1]
model = fasttext.train_unsupervised(filename, model="skipgram", dim=300)
filename = filename.replace(".txt", ".bin")
model.save_model(filename)
