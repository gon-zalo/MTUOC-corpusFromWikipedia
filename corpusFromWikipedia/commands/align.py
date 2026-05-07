# align functions
from ..utils.get_language import get_language

def score(x, y, fwd_mean, bwd_mean, margin):
    return margin(x.dot(y), (fwd_mean + bwd_mean) / 2)

def score_candidates(x, y, candidate_inds, fwd_mean, bwd_mean, margin):
    import numpy as np

    scores = np.zeros(candidate_inds.shape)
    for i in range(scores.shape[0]):
        for j in range(scores.shape[1]):
            k = candidate_inds[i, j]
            scores[i, j] = score(x[i], y[k], fwd_mean[i], bwd_mean[k], margin)
    return scores

def kNN(device, x, y, k, use_ann_search=False, ann_num_clusters=32768, ann_num_cluster_probe=3):
    import faiss
    import time
    start_time = time.time()
    
    if use_ann_search:
        # Mantinc la lògica GPU només per a la cerca aproximada (ANN)
        print("Perform approx. kNN search (GPU)")
        res = faiss.StandardGpuResources() 
        n_cluster = min(ann_num_clusters, int(y.shape[0]/1000))
        quantizer = faiss.IndexFlatIP(y.shape[1])
        index = faiss.IndexIVFFlat(quantizer, y.shape[1], n_cluster, faiss.METRIC_INNER_PRODUCT)
        gpu_index = faiss.index_cpu_to_gpu(res, 0, index)
        #sim, ind = index.search(x, k)
        #index.nprobe = ann_num_cluster_probe
        #index.train(y)
        #index.add(y)
        gpu_index.nprobe = ann_num_cluster_probe
        gpu_index.train(y)
        gpu_index.add(y)
        sim, ind = gpu_index.search(x, k)
    elif device == "gpu":
        res = faiss.StandardGpuResources()
        print("Perform exact search (GPU mode)")
        idx = faiss.IndexFlatIP(y.shape[1])
        gpu_index = faiss.index_cpu_to_gpu(res, 0, idx)
        #idx.add(y)
        #sim, ind = idx.search(x, k)
        gpu_index.add(y)
        sim, ind = gpu_index.search(x, k)
    elif device == "cpu":
        # MODIFICACIÓ: Forcem l'ús de la CPU per a la cerca exacta
        # Això evita l'error 'cublas failed' per falta de memòria VRAM
        print("Perform exact search (CPU Mode)")
        
        # Creem l'índex directament a la CPU
        index = faiss.IndexFlatIP(y.shape[1])
        
        # Afegim els vectors i busquem
        index.add(y)
        sim, ind = index.search(x, k)

    print("Done: {:.2f} sec".format(time.time()-start_time))
    return sim, ind

def file_open(filepath):
    #Function to allowing opening files based on file extension
    import gzip
    import lzma
    from pathlib import Path
    if Path(filepath).name.endswith('.gz'):
        return gzip.open(filepath, 'rt', encoding='utf-8')
    elif Path(filepath).name.endswith('xz'):
        return lzma.open(filepath, 'rt', encoding='utf-8')
    else:
        return open(filepath, 'r', encoding='utf-8')

def sentence_batches(filename, batch_size, min_len, max_len):
    batch = []

    with file_open(filename) as f:
        for line in f:
            line = line.strip()

            if min_len <= len(line) <= max_len:
                batch.append(line)

            if len(batch) >= batch_size:
                yield batch
                batch = []

    if batch:
        yield batch

def align_corpora(args):
    print("\nRunning align command")
    from sentence_transformers import SentenceTransformer, models
    import tqdm
    from sklearn.decomposition import PCA
    import torch
    from pathlib import Path
    import numpy as np
    import linecache
    import faiss
    import heapq


    device = args.device
    device = device.lower()

    if device == 'gpu' and not torch.cuda.is_available():
        print("GPU requested but not found. Using CPU.")
        device = 'cpu'
    indir = args.indir
    optional_indir = args.optional_indir

    indir_source = Path(indir)
    input_directories = [indir_source]

    if optional_indir:
        indir_target = Path(optional_indir)
        input_directories.append(indir_target)

    unique_segments_files = []
    unique_segments_files_codes = []
    print("")

    for indir in input_directories:
        for file in indir.iterdir():
            if file.is_file() and file.name.startswith("unique-segments"):
                print(f"Unique segments file {file.name} found")
                unique_segments_files.append(file)
                language_code = file.stem.split("-")
                unique_segments_files_codes.append(language_code[-1])

    source_file, target_file = unique_segments_files
    source_file_code, target_file_code = unique_segments_files_codes
    source_lang_name, source_lang_code = get_language(source_file_code)
    target_lang_name, target_lang_code = get_language(target_file_code)
    print(f"\nAligning {source_lang_name} and {target_lang_name}\n")

    outdir = args.outdir
    parallel_corpora_folder = indir_source.parent.parent / "parallel"

    if not parallel_corpora_folder.exists():
        parallel_corpora_folder.mkdir(parents=True, exist_ok=True)

    if not outdir:
        if len(input_directories) > 1: # if we are aligning separate corpora in different folders
            outdir = parallel_corpora_folder / f"aligned-{source_lang_code}-{target_lang_code}/"
            if not outdir.exists():
                outdir.mkdir(parents=True, exist_ok=True)
        else: # else just save the file in the same input directory
            outdir = indir

    #Model we want to use for bitext mining. LaBSE achieves state-of-the-art performance
    model_name = 'LaBSE'
    model = SentenceTransformer(model_name)

    # Only consider sentences that are between min_sent_len and max_sent_len characters long
    min_sent_len = 10
    max_sent_len = 200

    # We base the scoring on k nearest neighbors for each element
    knn_neighbors = 4

    # Min score for text pairs. Note, score can be larger than 1
    min_threshold = 1

    #Do we want to use exact search of approximate nearest neighbor search (ANN)
    #Exact search: Slower, but we don't miss any parallel sentences
    #ANN: Faster, but the recall will be lower
    use_ann_search = False #True

    #Number of clusters for ANN. Each cluster should have at least 10k entries
    ann_num_clusters = 32768

    #How many cluster to explorer for search. Higher number = better recall, slower
    ann_num_cluster_probe = 3

    #To save memory, we can use PCA to reduce the dimensionality from 768 to for example 128 dimensions
    #The encoded embeddings will hence require 6 times less memory. However, we observe a small drop in performance.
    use_pca = False #True
    pca_dimensions = 128


    if use_pca:
        print("Using PCA!")
        # We use a smaller number of training sentences to learn the PCA
        train_sent = []
        num_train_sent = 20000

        with file_open(source_file) as fSource, file_open(target_file) as fTarget:
            for line_source, line_target in zip(fSource, fTarget):
                if min_sent_len <= len(line_source.strip()) <= max_sent_len:
                    sentence = line_source.strip()
                    train_sent.append(sentence)

                if min_sent_len <= len(line_target.strip()) <= max_sent_len:
                    sentence = line_target.strip()
                    train_sent.append(sentence)

                if len(train_sent) >= num_train_sent:
                    break

        print("Encode training embeddings for PCA")
        train_matrix = model.encode(train_sent, show_progress_bar=True, convert_to_numpy=True)
        pca = PCA(n_components=pca_dimensions)
        pca.fit(train_matrix)

        dense = models.Dense(in_features=model.get_sentence_embedding_dimension(), out_features=pca_dimensions, bias=False, activation_function=torch.nn.Identity())
        dense.linear.weight = torch.nn.Parameter(torch.tensor(pca.components_))
        model.add_module('dense', dense)


    EMBED_DIM = 768
    BATCH_SIZE = 10000

    print("Building FAISS index (target side)")

    index_y = faiss.IndexFlatIP(EMBED_DIM)

    target_store_path = outdir / f"target-sentences-{target_file_code}.txt"

    with open(target_store_path, "w", encoding="utf-8") as fStore:

        for batch in tqdm.tqdm(
            sentence_batches(target_file, BATCH_SIZE, min_sent_len, max_sent_len)
        ):

            emb = model.encode(
                batch,
                batch_size=32,
                convert_to_numpy=True,
                normalize_embeddings=True,
                show_progress_bar=False
            ).astype(np.float32)

            index_y.add(emb)

            for s in batch:
                fStore.write(s.replace("\n", " ") + "\n")

    heap = []
    TOP_K_GLOBAL = 5_000_000  # optional tuning 

    print("Streaming source + collecting candidates")

    src_offset = 0

    margin = lambda a, b: a / b

    for src_batch in tqdm.tqdm(
        sentence_batches(source_file, BATCH_SIZE, min_sent_len, max_sent_len)
    ):

        x = model.encode(
            src_batch,
            batch_size=32,
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False
        ).astype(np.float32)

        # FAISS retrieval
        x2y_sim, x2y_ind = index_y.search(x, knn_neighbors)
        x2y_mean = x2y_sim.mean(axis=1)

        # approximate reverse consistency (batch-local)
        y2x_sim, y2x_ind = index_y.search(x, knn_neighbors)
        y2x_mean = y2x_sim.mean(axis=1)

        # your original scoring logic
        fwd_scores = score_candidates(
            x, None,
            x2y_ind,
            x2y_mean,
            y2x_mean,
            margin
        )

        for i in range(len(src_batch)):

            best_score = float(fwd_scores[i].max())

            if best_score < min_threshold:
                continue

            trg_idx = int(x2y_ind[i][fwd_scores[i].argmax()])

            heapq.heappush(heap, (
                -best_score,
                src_offset + i,
                trg_idx
            ))

        src_offset += len(src_batch)

        del x
        
    print("Final global selection")

    seen_src = set()
    seen_trg = set()

    outfile = outdir / f"aligned-{source_file_code}-{target_file_code}.txt"

    heapq.heapify(heap)

    with open(outfile, "w", encoding="utf-8") as fOut:

        while heap:

            neg_score, src_id, trg_id = heapq.heappop(heap)
            score = -neg_score

            if src_id in seen_src or trg_id in seen_trg:
                continue

            seen_src.add(src_id)
            seen_trg.add(trg_id)

            src_text = linecache.getline(source_file, src_id + 1).strip()
            trg_text = linecache.getline(target_store_path, trg_id + 1).strip()

            fOut.write(
                f"{src_text}\t{trg_text}\t{score:.4f}\n"
            )