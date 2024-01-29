import pandas as pd
remove_workshops = True

display_settings =(
    'display.max_rows', None,
    'display.max_columns', None,
    'display.width', None,
    # Allow long column width for venue
    'max_colwidth', 1000
)


def get_venues_df():
    return pd.read_csv("venues.csv")


def filter_workshops(df):
    if not remove_workshops:
        return df
    df = df[~df['venue'].str.contains("Workshop", case=False)]
    # filter if contains "@"
    df = df[~df['venue'].str.contains("@", case=False)]
    return df


def get_icse_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = df[df['venue'].str.contains("Software Engineering", case=False)]
    df = filter_workshops(df)
    # Search for "International Conference on Software Engineering" or "ICSE"
    df = df[df['venue'].str.contains("International Conference on Software Engineering|ICSE", case=False)]
    exclude_strings = [
        "International Conference on Software Engineering Advances",
        "Artificial Intelligence, Networking and Parallel/Distributed Computing",
        "CONSEG",
        "Software Engineering and Service Science",
        "ICSOFT-EA",
        "International Conference on Software Engineering and Service Science",
        "CSEET",
        "ICSESS",
        "ICSECS",
        "International Conference on Software Engineering and Formal Methods",
        " International Conference on Software Engineering Advances",
        "International Conference on Software Engineering Research and Applications",
        "International Conference on Software Engineering and New Technologies",
        "Proceedings of the International Conference on Software Engineering and Knowledge Engineerin",
        "2008 ACM/IEEE 30th International Conference on Software Engineering",
        "ICICSE",
        "SEAI",
        "International Conference on Software Engineering for Defence Applications",
        "Knowledge Engineering",
        "Majorov",
        "SEAI",
        "International Conference on Software Engineering and Computer Systems",
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(
        'display.max_rows', None,
        'display.max_columns', None,
        'display.width', None,
        # Allow long column width for venue
        'max_colwidth', 1000
    ):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_ase_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = df[df['venue'].str.contains("Software Engineering", case=False)]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("Automated Software Engineering", case=False)]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue

def get_fse_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("Foundations of Software Engineering| FSE", case=False)]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_issta_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    #df = df[df['venue'].str.contains("Software Engineering", case=False)]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("International Symposium on Software Testing and Analysis|ISSTA", case=False)]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_se_conference_names():
    return [
        *get_ase_names(),
        *get_icse_names(),
        *get_issta_names(),
        *get_fse_names(),
    ]

## AI

def get_aaai_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("AAAI", case=False)]
    # TODO: include symposiums? Include ethics and society?
    df = df[df['venue'].str.contains("AAAI Conference on Artificial Intelligence", case=False)]
    df = df[~df['venue'].str.contains("Interactive Digital Entertainment", case=False)]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_ijcai_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("IJCAI|International Joint Conference on Artificial Intelligence", case=False)]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_ai_conference_names():
    return [
        *get_aaai_names(),
        *get_ijcai_names(),
    ]


## ML

def get_neurips_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("NeurIPS|NIPS|Neural Information Processing Systems", case=False)]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_iclr_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("ICLR|International Conference on Learning Representations", case=False)]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_icml_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("ICML|International Conference on Machine Learning", case=False)]
    df = df[df['venue'] == "International Conference on Machine Learning"]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_kdd_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("KDD|Knowledge Discovery and Data Mining", case=False)]
    df = df[df['venue'] == "Knowledge Discovery and Data Mining"]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_ml_conference_names():
    return [
        *get_neurips_names(),
        *get_iclr_names(),
        *get_icml_names(),
        *get_kdd_names(),
    ]


## CV

def get_cvpr_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("CVPR|Computer Vision and Pattern Recognition", case=False)]
    #df = df[df['venue'] == "Computer Vision and Pattern Recognition"]
    exclude_strings = [
        "Energy Minimization Methods",
        "Advances in Computer Vision and Pattern Recognition",
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_eccv_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("ECCV|European Conference on Computer Vision", case=False)]
    #df = df[df['venue'] == "European Conference on Computer Vision"]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_iccv_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("ICCV|International Conference on Computer Vision", case=False)]
    #df = df[df['venue'] == "International Conference on Computer Vision"]
    exclude_strings = [
        "Computer Vision in Remote Sensing",
        "CVIDL",
        "International Conference on Computer Vision Theory and Applications",
        "Computer Graphics Collaboration Techniques",
        "Proceedings of 3rd International Conference on Computer Vision and Image Processing",
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_cv_conference_names():
    return [
        *get_cvpr_names(),
        *get_eccv_names(),
        *get_iccv_names(),
    ]


# NLP

def get_acl_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("Annual Meeting of the Association for Computational Linguistics", case=False)]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_emnlp_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("EMNLP|Conference on Empirical Methods in Natural Language Processing", case=False)]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_naacl_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("NAACL|North American Chapter of the Association for Computational Linguistics", case=False)]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue

def get_nlp_conference_names():
    return [
        *get_acl_names(),
        *get_emnlp_names(),
        *get_naacl_names(),
    ]


# Def get SE jornals

def get_tse_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = df[df['venue'].str.contains("IEEE Transactions on Software Engineering", case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_jss_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = df[df['venue'].str.contains("Journal of Systems and Software", case=False)]
    exclude_strings = [
        "International Journal of Systems and Software Security and Protection",
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_ese_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = df[df['venue'] == 'Empirical Software Engineering']
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_ist_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = df[df['venue'].str.contains("Information and Software Technology", case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_tosem_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = df[df['venue'].str.contains("Transactions on Software Engineering and Methodology", case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_se_journal_names():
    return [
        *get_tse_names(),
        *get_jss_names(),
        *get_ist_names(),
        *get_ese_names(),
        *get_tosem_names(),
    ]


# PL

def get_oopsla_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("OOPSLA|Object-Oriented Programming Systems, Languages, and Applications", case=False)]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_popl_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("POPL|Principles of Programming Languages", case=False)]
    exclude_strings = [
        "Modelling of Soil Behaviour with Hypoplasticity"
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_pldi_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("PLDI|Programming Language Design and Implementation", case=False)]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_icfp_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = filter_workshops(df)
    df = df[df['venue'].str.contains("ICFP|International Conference on Functional Programming", case=False)]
    exclude_strings = [
    ]
    for exclude_string in exclude_strings:
        df = df[~df['venue'].str.contains(exclude_string, case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue


def get_pl_conference_names():
    return [
        *get_oopsla_names(),
        *get_popl_names(),
        *get_pldi_names(),
        *get_icfp_names(),
    ]


## Arxiv

def get_arxiv_names():
    df = get_venues_df()
    df = df[df['venue'].notna()]
    df = df[df['venue'].str.contains("arxiv|arxiv.org", case=False)]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return list(df.venue) + ['arXiv.org']


def get_unkown_names():
    df = get_venues_df()
    df = df[df['venue'].isna() | (df['venue'] == "")]
    with pd.option_context(*display_settings):
        print(df)
        print(df['corpusid'].sum())
    return df.venue



def main():
    #se_conference_names = get_se_conference_names()
    #print(se_conference_names)
    se_journal_names = get_se_journal_names()
    print(se_journal_names)


if __name__ == "__main__":
    main()