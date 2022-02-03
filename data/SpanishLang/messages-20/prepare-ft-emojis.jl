# run locally
using TextSearch

include(joinpath(ENV["JULIA_PROJECT"], "src/io.jl"))

const VALID = Set(['😄', '😍', '😘', '😊', '😌', '🥺', '😡', '😒', '😭', '😢',  '❤', '💔', '👌', '👏', '🤔'])

function create_emoji_dataset(filename)
    trainfile = replace(filename, ".txt" => "") * "-emojis-train.txt"
    testfile = replace(filename, ".txt" => "") * "-emojis-test.txt"
    @info filename => [trainfile, testfile]
    #isfile(outfile) && return
    s = 0
    train = open(trainfile, "w")
    test = open(testfile, "w")
    F = [train, test]
    for line in eachline(filename)
        emolist = emojis(line)
        e = intersect(VALID, emolist)
        if length(e) == 1
            e = first(e)
            line = replace(line, e => " _emo ")
            println(rand(F), "__label__", e, ' ', line)
            s += 1
        end
    end
    
    close(train)
    close(test)
end

for filename in ARGS
    create_emoji_dataset(filename)
end
