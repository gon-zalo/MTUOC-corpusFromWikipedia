# select functions
def select_corpus(args):
    print("Selecting segments from file...")
    input_file = args.input
    output_file = args.output
    sl = args.sl
    tl = args.tl
    sldc = args.sldc
    tldc = args.tldc
    minSBERT = args.minSBERT

    if minSBERT == None:
        minSBERT = -1000000
    else:
        minSBERT = float(minSBERT)


    sortida=open(output_file,"w",encoding="utf-8")
    entrada=open(input_file,"r",encoding="utf-8")

    for linia in entrada:
        linia=linia.rstrip()
        camps=linia.split("\t")
        slsegment=camps[0]
        tlsegment=camps[1]
        slinfolangs=camps[2]
        slinfolang1=slinfolangs.split(";")[0]
        sllang=slinfolang1.split(":")[0]
        slconf=float(slinfolang1.split(":")[1])
        
        tlinfolangs=camps[3]
        tlinfolang1=tlinfolangs.split(";")[0]
        tllang=tlinfolang1.split(":")[0]
        tlconf=float(tlinfolang1.split(":")[1])
        
        sbert=float(camps[4])
        
        if sllang==sl and slconf>=sldc and tllang==tl and tlconf>=tldc and sbert>=minSBERT:
            cadena=slsegment+"\t"+tlsegment
            #print(cadena)
            sortida.write(cadena+"\n")
