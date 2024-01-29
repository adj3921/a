from pathlib import Path
import shutil
import gzip
import pandas as pd
from itertools import islice
from pprint import pprint
import dask.dataframe as dd
import dask.bag as db
import dask

from proc_papersmetad import make_sample_file, read_cs_papers

cur_path = Path(__file__).parent.absolute()
raw_files = cur_path / "data/s2orc/raw"
use_sample = False


def yield_s2orc_papers(data_path: Path = raw_files / "s2orc_2023-03-07_0.jsonl"):
    # yield each paper in the jsonl file
    import json
    with open(data_path) as f:
        for line in f:
            yield json.loads(line)


def load_dd_papers():
    return dd.read_json(
        str(raw_files / ("s2orc_2023-03-07_0" + (".sample" if use_sample else "") + ".jsonl")),
        lines=True,
        blocksize=2 ** 28
    )


def load_db_papers():
    return db.read_text(
        str(raw_files / ("s2orc_2023-03-07_0" + (".sample" if use_sample else "") + ".jsonl")),
        #blocksize=2 ** 28
    )


def load_dd_s2orc_idv(idx: int):
    # load the papers metadata using dask into a dask dataframe
    file = raw_files / (f"s2orc_2023-03-07_{idx}" + (".sample" if use_sample else "") + ".jsonl.gz")
    dest_file = raw_files / (f"s2orc_2023-03-07_{idx}" + (".sample" if use_sample else "") + ".jsonl")
    print("unzipping file: ", file)
    if not dest_file.exists():
        import mgzip
        with mgzip.open(str(file), 'rb', thread=32) as f_in:
            with open(dest_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    print("done unzipping file: ", file)
    return (
        dd.read_json(
            raw_files / (f"s2orc_2023-03-07_{idx}" + (".sample" if use_sample else "") + ".jsonl"),
            lines=True,
            blocksize=2 ** 28,
        ),
        dest_file
    )


def read_cs_s2orc():
    return dd.read_parquet(
        str(raw_files / (f"../cs_s2orc_parquet/cs_s2orc_2023-03-07_all.parquet")),
    )
    #return dd.read_json(
    #    raw_files / (f"../cs_s2orc/cs_s2orc_2023-03-07_*_p*.jsonl.gz"
    #                 + (".sample" if use_sample else "")),
    #    lines=True,
    #    #blocksize=2 ** 28 / 8
    #)


def _run_save(i, papers_index):
    print("processing file: ", i, " of 30")
    ddf, unziped_file = load_dd_s2orc_idv(i)
    # ddf = ddf.repartition(npartitions=ddf.npartitions * 8)
    ddf = ddf[ddf['corpusid'].isin(papers_index)]
    # ddf = ddf.repartition(npartitions=max(ddf.npartitions // 16, 1))
    print(ddf)
    # ddf.to_parquet(
    #    raw_files / (f"../s2orc_2023-03-07_{i}" + (".sample" if use_sample else "") + ".parquet")
    # )
    #
    ddf.to_json(
        raw_files / (f"../cs_s2orc/cs_s2orc_2023-03-07_{i}_p*.jsonl.gz"
                     + (".sample" if use_sample else "")),
        orient='records',
        lines=True,
        compression='gzip',
    )
    # clean up the unzipped file
    if i > 0:
        print("removing file: ", unziped_file)
        unziped_file.unlink()


def save_s2orc_cs_papers():
    dask.config.set({
        "distributed.worker.memory.target": 0.3 / 2,
        "distributed.worker.memory.terminate": 0.8 / 2,
        "distributed.worker.memory.spill": 0.6 / 2,
        "distributed.worker.memory.pause": 0.75 / 2,
    })
    #from dask.distributed import client
    #client = client(n_workers=int(128/4), threads_per_worker=1, memory_limit='4gb')
    papers_df = read_cs_papers()
    print("making paper index")
    papers_index = pd.index(papers_df['corpusid'].compute())
    failed_files = []
    for i in range(30):
        if i != 10:
            continue  # done
        try:
            _run_save(i, papers_index)
        except Exception as e:
            print("error processing file: ", i, " of 30")
            print(e)
            failed_files.append(i)
    print("failed files: ", failed_files)
    
    
def _get_schema():
    from pyarrow import schema, field, list_, struct
    return schema([
        field("corpusid", "int64"),
        ('externalids', struct([
            field('ACL', 'string'),
            field('ArXiv', 'string'),
            field('CorpusId', 'string'),
            field('DBLP', 'string'),
            field('DOI', 'string'),
            field('MAG', 'string'),
            field('PubMed', 'string'),
            field('PubMedCentral', 'string')
        ])),
        field("content", "string"),
        field("updated", "string"),
        #field("venue", "string"),
    ])


def all_s2orc_to_arrow():
    papers_df = read_cs_papers()
    ddf = read_cs_s2orc()
    # add the 'venue' col to ddf from papers_df matching on corpusid
    # first make indexes for both
    #papers_df = papers_df.set_index('corpusid', npartitions='auto')
    #ddf = ddf.set_index('corpusid', npartitions='auto')
    #ddf = ddf.merge(papers_df[['corpusid', 'venue']], on='corpusid', how='left')
    #print(ddf[ddf['corpusid'].apply(
    #    lambda x: not x.is_integer()
    #    , meta=('corpusid', 'int64')
    #)].compute())
    #print(ddf)
    #vals = ddf['corpusid'].compute()
    #for v in list(vals):
    #    try:
    #        if type(v) == int:
    #            continue
    #        if not v.is_integer():
    #            print(v)
    #    except Exception as e:
    #        print(e)
    #        print(v)
    ##print(vals)
    #exit()

    ddf['content'] = ddf['content'].astype(str)
    ddf['corpusid'] = ddf['corpusid'].astype('int64')
    #ddf = ddf.set_index('corpusid', npartitions='auto')
    #ddf['updated'] = ddf['updated'].astype(str)

    #ddf = ddf.repartition(npartitions=ddf.npartitions // 8)
    print(ddf)
    ddf.to_parquet(
        raw_files / (f"../cs_s2orc_parquet/cs_s2orc_2023-03-07_all.parquet"),
        schema=_get_schema(),
        #engine='pyarrow',
    )


def explore_venues():
    content_ddf = read_cs_s2orc()
    papers_df = read_cs_papers()
    print("Computing index")
    content_corpusids = pd.Index(content_ddf['corpusid'].compute())
    print("Computing overlap")
    papers_df['has_content'] = papers_df['corpusid'].isin(content_corpusids)
    # compute the fraction that 'has_content' for each venue
    print("Computing fraction")
    frac_df = papers_df.groupby('venue').agg({'has_content': 'mean'})
    frac_df = frac_df.persist()
    print(frac_df)
    # save the frac_df
    frac_df.to_csv(raw_files / "../cs_s2orc_2023-03-07_venue_frac.csv")


def save_venue_count_by_year():
    papers_df = read_cs_papers()
    papers_df = papers_df.persist()
    venue_counts = papers_df.groupby(['venue', 'year']).agg({'corpusid': 'count'})
    venue_counts = venue_counts.persist()
    print(venue_counts)
    venue_counts.to_csv(raw_files / "../cs_s2orc_2023-03-07_venue_counts.csv")


def find_fse():
    papers_df = read_cs_papers()
    papers_df = papers_df.persist()
    # Find all venues that contain "Foundations of Software Engineering"
    #papers_df = papers_df[papers_df['venue'].str.contains(
    #    "Foundations of Software Engineering",
    #)]
    papers_df = papers_df[papers_df['title'].str.contains(
        "Understanding GCC builtins to develop better tools"
    )]
    venue_counts = papers_df.groupby(['venue', 'year']).agg({'corpusid': 'count'})
    venue_counts = venue_counts.persist()
    print(venue_counts)
    venue_counts.to_csv(raw_files / "../cs_s2orc_2023-03-07_fse_why.csv")


def explore_contents():
    content_ddf = read_cs_s2orc()
    search_term = "AI safety"
    # search for the term in the content
    print("Searching for term: ", search_term)
    #content_ddf = content_ddf.sample(frac=0.01)
    search_df = content_ddf[content_ddf['content'].str.contains(
        search_term,
        case=False,
        regex=False,
    )]
    print("Computing index")
    search_corpusids = pd.Index(search_df['corpusid'].compute())
    print("Computing overlap")
    papers_df = read_cs_papers()
    papers_df['has_content'] = papers_df['corpusid'].isin(search_corpusids)
    papers_df = papers_df[papers_df['has_content']]
    # gather the title and venue and put in a csv
    print("Computing fraction")
    result_df = papers_df[['title', 'venue']]
    result_df = result_df.repartition(npartitions=1)
    result_df = result_df.persist()
    print(result_df)
    result_df.to_csv(
        raw_files / f"../cs_s2orc_2023-03-07_search_{search_term}_results_*.csv",
    )






def main():
    #make_sample_file(raw_files / "s2orc_2023-03-07_0.jsonl", frac=0.01)
    import sys
    # take in argv for index to save
    if len(sys.argv) > 1:
        idx = int(sys.argv[1])
        #print("WOO {}".format(idx))
        papers_df = read_cs_papers()
        papers_index = pd.Index(papers_df['corpusid'].compute())
        _run_save(idx, papers_index)
        exit()

    #save_s2orc_cs_papers()
    #all_s2orc_to_arrow()
    #explore_venues()
    #explore_contents()
    #save_venue_count_by_year()
    find_fse()

    #ddf = read_cs_s2orc()
    #print(len(ddf))
    exit()


    ddf = load_dd_papers()
    papers_df = read_cs_papers()
    #papers_df = papers_df.set_index('corpusid')
    #ddf = ddf.set_index('corpusid')
    ddf = ddf[ddf['corpusid'].isin(pd.Index(papers_df['corpusid'].compute()))]
    print(ddf)
    print(len(ddf))
    exit()

    papers = yield_s2orc_papers()
    dates = []
    for paper in islice(papers, 5):
        pprint(paper)
        #dates.append(paper['updated'])
    dates.sort()
    print(dates[0], dates[-1])


if __name__ == "__main__":
    main()