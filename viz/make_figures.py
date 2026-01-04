from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


ROOT = Path(__file__).resolve().parents[1]
VIZ_DIR = ROOT / "output" / "viz"
FIG_DIR = ROOT / "output" / "figures"


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8")


def _save(fig: plt.Figure, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=200, bbox_inches="tight")
    plt.close(fig)


def _barh_top(df: pd.DataFrame, label_col: str, value_col: str, *, top_n: int, title: str) -> plt.Figure:
    df = df[[label_col, value_col]].dropna().copy()
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[value_col])
    df = df.sort_values(value_col, ascending=False).head(top_n)

    fig, ax = plt.subplots(figsize=(10, max(4, 0.35 * len(df))))
    sns.barplot(data=df, y=label_col, x=value_col, ax=ax, color=sns.color_palette()[0])
    ax.set_title(title)
    ax.set_xlabel(value_col)
    ax.set_ylabel(label_col)
    ax.grid(axis="x", alpha=0.25)
    return fig


def _bar(df: pd.DataFrame, label_col: str, value_col: str, *, title: str) -> plt.Figure:
    df = df[[label_col, value_col]].dropna().copy()
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[value_col])
    df = df.sort_values(value_col, ascending=False)

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.barplot(data=df, x=label_col, y=value_col, ax=ax, color=sns.color_palette()[0])
    ax.set_title(title)
    ax.set_xlabel(label_col)
    ax.set_ylabel(value_col)
    ax.tick_params(axis="x", rotation=30)
    ax.grid(axis="y", alpha=0.25)
    return fig


def _heatmap_share(df: pd.DataFrame, *, top_provinces: int = 25, top_industries: int = 20) -> plt.Figure:
    # Expect columns: province, industry, job_count, province_job_count, share
    df = df.copy()
    for c in ["job_count", "province_job_count", "share"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Choose top provinces by province_total and top industries by total job_count
    top_prov = (
        df[["province", "province_job_count"]]
        .dropna()
        .drop_duplicates()
        .sort_values("province_job_count", ascending=False)
        .head(top_provinces)["province"]
    )

    top_ind = (
        df.groupby("industry", as_index=False)["job_count"].sum()
        .sort_values("job_count", ascending=False)
        .head(top_industries)["industry"]
    )

    sub = df[df["province"].isin(top_prov) & df["industry"].isin(top_ind)].copy()
    pivot = sub.pivot_table(index="province", columns="industry", values="share", aggfunc="sum", fill_value=0.0)

    # Order provinces by total descending
    prov_order = (
        df[["province", "province_job_count"]]
        .drop_duplicates()
        .set_index("province")
        .loc[pivot.index]["province_job_count"]
        .sort_values(ascending=False)
        .index
    )
    pivot = pivot.loc[prov_order]

    fig, ax = plt.subplots(figsize=(max(10, 0.4 * pivot.shape[1]), max(6, 0.35 * pivot.shape[0])))
    sns.heatmap(pivot, ax=ax, cmap="Blues", cbar_kws={"label": "share"})
    ax.set_title(f"Province × Industry share (top {top_provinces} provinces, top {top_industries} industries)")
    ax.set_xlabel("industry")
    ax.set_ylabel("province")
    return fig


def main() -> int:
    sns.set_theme(style="whitegrid")
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    outputs: list[Path] = []

    # 1) Overall industries (known)
    p = VIZ_DIR / "industry_total_known.csv"
    if p.exists():
        df = _read_csv(p)
        fig = _barh_top(df, "industry", "job_count", top_n=25, title="Top industries (known) by job_count")
        out = FIG_DIR / "industry_total_known_top25.png"
        _save(fig, out)
        outputs.append(out)

    # 2) Province totals
    p = VIZ_DIR / "province_total.csv"
    if p.exists():
        df = _read_csv(p)
        fig = _barh_top(df, "province", "province_job_count", top_n=25, title="Top provinces by job_count")
        out = FIG_DIR / "province_total_top25.png"
        _save(fig, out)
        outputs.append(out)

    # 3) Province × Industry share heatmap
    p = VIZ_DIR / "province_industry_share.csv"
    if p.exists():
        df = _read_csv(p)
        if {"province", "industry", "job_count", "province_job_count", "share"}.issubset(df.columns):
            fig = _heatmap_share(df, top_provinces=25, top_industries=20)
            out = FIG_DIR / "province_industry_share_heatmap.png"
            _save(fig, out)
            outputs.append(out)

    # 4) Requirement distributions
    reqs = [
        ("requirement_experience_total.csv", "experience", "job_count", "Experience distribution"),
        ("requirement_education_total.csv", "education", "job_count", "Education distribution"),
        ("requirement_employment_type_total.csv", "employment_type", "job_count", "Employment type distribution"),
    ]
    for filename, label_col, value_col, title in reqs:
        p = VIZ_DIR / filename
        if not p.exists():
            continue
        df = _read_csv(p)
        fig = _bar(df, label_col, value_col, title=title)
        out = FIG_DIR / filename.replace(".csv", ".png")
        _save(fig, out)
        outputs.append(out)

    # 5) Top skills
    p = VIZ_DIR / "requirement_skill_total_top500.csv"
    if p.exists():
        df = _read_csv(p)
        fig = _barh_top(df, "skill", "job_count", top_n=30, title="Top skills by job_count")
        out = FIG_DIR / "requirement_skill_top30.png"
        _save(fig, out)
        outputs.append(out)

    if outputs:
        print("Wrote figures:")
        for o in outputs:
            print(f"- {o.relative_to(ROOT)}")
        return 0

    print(f"No expected CSVs found in: {VIZ_DIR}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
