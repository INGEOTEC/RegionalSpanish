import fasttext as ft
import os
import sys

filename = sys.argv[1]
outname = filename.replace(".bin", ".vec")
m = ft.load_model(filename)

with open(outname, "w") as f:
    X = m.get_output_matrix()
    n, d = X.shape
    vocab = m.get_words()
    print("{} {}".format(n, d), file=f)
    for i in range(n):
        print(vocab[i], " ", " ".join(map(str, X[i])), file=f)

len(m.get_words())
m.get_output_matrix()
m.get_output_matrix().shape
