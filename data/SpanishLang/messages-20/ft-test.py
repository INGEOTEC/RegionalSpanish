import sys
import os
from glob import glob
import fasttext

def vecmodel(modelname, vecname):
    model = fasttext.load_model(modelname)
    words = model.get_words()

    with open(vecname, 'w') as f:
        f.write("{} {}\n".format(len(words), model.get_dimension()))

        for w in words:
            f.write(w)
            for c in model.get_word_vector(w):
                f.write(" {:0.5f}".format(c))
            f.write('\n')

def main_vecmodel():
    print(sys.argv)
    for modelname in sys.argv[1:]:
        print(modelname)
        vecname = os.path.basename(modelname).replace(".bin", "") + ".vec"
        if not os.path.isfile(vecname):
            vecmodel(modelname, vecname)

def main_prepare():
    for vmodel in glob("*.vec"):
        for train in glob("*-train.txt"):
            print("srun -c4 --mem-per-cpu=12000 python {} {} {} &".format(sys.argv[0], train, vmodel))
    
    print("wait")

def main(train, pretrained):
    outname = "output.{}-{}.data".format(train, pretrained)
    if os.path.isfile(outname):
        return
        
    model = fasttext.train_supervised(train, epoch=5, thread=14, wordNgrams=3, dim=300, pretrainedVectors=pretrained)
    #model = fasttext.load_model("../embeddings/MX.bin")
    test = train.replace("-train", "-test")
    N, p, r = model.test(test)
    with open(outname, "w") as f:
        f.write("{}\t{}\t{}\t{:.4f}\t{:.4f}\n".format(train, pretrained, N, p, r))

#main_vecmodel()
#main_prepare()
main(sys.argv[1], sys.argv[2])
