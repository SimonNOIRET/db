import streamlit as st
import pandas as pd
import pyodbc
import plotly.express as px
from io import BytesIO

ACCESS_DB_PATH = r"C:\Users\Simon\Documents\ArkeaAM\VSCode\lexifi_mkt_data.accdb"
TABLE_NAME = "asset_spot"
ID_COL = "lexifi_id"
DATE_COL = "lexifi_date"
VALUE_COL = "lexifi_spot"

@st.cache_data
def connect_and_fetch_ids():
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={ACCESS_DB_PATH};'
    )
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(f"SELECT DISTINCT {ID_COL} FROM {TABLE_NAME}", conn)
    conn.close()
    return df[ID_COL].dropna().astype(str).tolist()

@st.cache_data
def fetch_data_for_id(selected_id):
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={ACCESS_DB_PATH};'
    )
    conn = pyodbc.connect(conn_str)
    query = f"""
        SELECT {DATE_COL}, {VALUE_COL}
        FROM {TABLE_NAME}
        WHERE {ID_COL} = ?
        ORDER BY {DATE_COL}
    """
    df = pd.read_sql(query, conn, params=[selected_id])
    conn.close()
    df[DATE_COL] = pd.to_datetime(df[DATE_COL])
    df["id"] = selected_id
    return df

st.set_page_config(page_title="Données de marché", layout="wide")
st.title("📈 Visualisation des données 'asset spot'")
st.caption("Source: LexiFi")

id_list = connect_and_fetch_ids()
selected_ids = st.multiselect("Sélectionner un ou plusieurs IDs :", id_list, key="id_selector")

if selected_ids:
    all_data = []
    for id_ in selected_ids:
        df = fetch_data_for_id(id_)
        if not df.empty:
            all_data.append(df)

    if all_data:
        combined_df = pd.concat(all_data)
        combined_df = combined_df.pivot(index=DATE_COL, columns="id", values=VALUE_COL)
        combined_df = combined_df.sort_index().ffill()

        full_min_date = combined_df.index.min().date()
        full_max_date = combined_df.index.max().date()

        common_min_date = pd.to_datetime("today")
        for col in combined_df.columns:
            first_valid = combined_df[col].first_valid_index()
            if first_valid is not None and first_valid < common_min_date:
                common_min_date = first_valid

        if "start_date" not in st.session_state or st.session_state.get("last_ids") != selected_ids:
            st.session_state.start_date = common_min_date.date()
            st.session_state.last_ids = selected_ids

        start_date = st.date_input(
            "Choisir une date de départ pour l’affichage :",
            value=st.session_state.start_date,
            min_value=full_min_date,
            max_value=full_max_date,
            key="start_date_input"
        )
        st.session_state.start_date = start_date

        filtered_df = combined_df[combined_df.index >= pd.to_datetime(start_date)]

        if filtered_df.dropna(how='all').empty:
            st.warning("⚠️ Aucune donnée disponible pour les séries sélectionnées à partir de cette date.")
        else:
            try:
                if len(selected_ids) > 1:
                    rebased_df = filtered_df.copy()
                    excluded_series = []
                    for col in rebased_df.columns:
                        first_valid = rebased_df[col].first_valid_index()
                        if first_valid:
                            base_value = rebased_df.loc[first_valid, col]
                            rebased_df[col] = (rebased_df[col] / base_value) * 100
                            rebased_df[col].loc[:first_valid] = 100
                        else:
                            excluded_series.append(col)

                    if excluded_series:
                        st.info(f"ℹ️ Séries exclues (aucune donnée après {start_date}) : {', '.join(excluded_series)}")
                        rebased_df.drop(columns=excluded_series, inplace=True)

                    plot_df = rebased_df.reset_index().melt(id_vars=DATE_COL, var_name="ID", value_name="Valeur")
                    chart_title = f"Séries rebasées à 100 à partir du {start_date}"
                else:
                    plot_df = filtered_df.reset_index().melt(id_vars=DATE_COL, var_name="ID", value_name="Valeur")
                    chart_title = f"Données pour l'identifiant : {selected_ids[0]} à partir du {start_date}"

                if plot_df.empty:
                    st.warning("⚠️ Données introuvables pour cette configuration.")
                else:
                    fig = px.line(
                        plot_df,
                        x=DATE_COL,
                        y="Valeur",
                        color="ID",
                        title=chart_title,
                    )

                    fig.update_traces(
                        line=dict(width=1),
                        marker=dict(size=4),
                        mode="lines+markers"
                    )

                    fig.update_layout(
                        xaxis_title="Date",
                        yaxis_title="Valeur",
                        hovermode="x unified",
                        height=800,
                        width=900,
                        legend=dict(orientation="v", x=1.02, y=1, xanchor="left")
                    )

                    st.plotly_chart(fig, use_container_width=True)

                    raw_plot_df = filtered_df.reset_index().melt(id_vars=DATE_COL, var_name="ID", value_name="Valeur")
                    csv_buffer = BytesIO()
                    export_df = raw_plot_df.rename(columns={DATE_COL: "Date"})
                    export_df.to_csv(csv_buffer, index=False)
                    st.download_button(
                        label="📁 Télécharger les données affichées en CSV",
                        data=csv_buffer.getvalue(),
                        file_name="donnees_visualisation.csv",
                        mime="text/csv"
                    )

                    st.subheader("📊 Statistiques des séries sélectionnées")
                    stats_df = filtered_df.copy()
                    last_date = stats_df.index.max()
                    start_year = pd.to_datetime(start_date).year
                    end_year = last_date.year
                    years = list(range(start_year, end_year + 1))
                    stats_summary = []

                    for col in stats_df.columns:
                        serie = stats_df[col].dropna()
                        if serie.empty:
                            continue

                        val_current = serie.iloc[-1]
                        val_min = serie.min()
                        val_max = serie.max()

                        perf_by_year = {}
                        for year in years:
                            dec_31 = pd.Timestamp(f"{year-1}-12-31")
                            end_of_year_data = serie[serie.index.year == year]
                            if not end_of_year_data.empty:
                                try:
                                    start_val = serie[serie.index <= dec_31].iloc[-1]
                                    end_val = end_of_year_data.iloc[-1]
                                    perf = (end_val / start_val - 1) * 100
                                    perf_by_year[f"Perf {year}"] = perf
                                except:
                                    perf_by_year[f"Perf {year}"] = None
                            else:
                                perf_by_year[f"Perf {year}"] = None

                        dec_31_last_year = pd.Timestamp(f"{last_date.year - 1}-12-31")
                        try:
                            ytd_start_val = serie[serie.index <= dec_31_last_year].iloc[-1]
                            perf_ytd = (val_current / ytd_start_val - 1) * 100
                        except:
                            perf_ytd = None

                        stats_summary.append({
                            "ID": col,
                            "Valeur actuelle": val_current,
                            "Min": val_min,
                            "Max": val_max,
                            **perf_by_year,
                            "Perf YTD": perf_ytd
                        })

                    stats_table = pd.DataFrame(stats_summary)

                    def format_number(x):
                        return f"{x:,.2f}".replace(",", " ").replace(".00", ".00")

                    def format_percent(x):
                        return f"{x:.2f} %" if pd.notna(x) else "-"

                    format_dict = {
                        "Valeur actuelle": format_number,
                        "Min": format_number,
                        "Max": format_number,
                        "Perf YTD": format_percent
                    }
                    for year in years:
                        format_dict[f"Perf {year}"] = format_percent

                    stats_table = stats_table.sort_values(by="Perf YTD", ascending=False)
                    st.dataframe(
                        stats_table.style.format(format_dict).set_properties(**{'text-align': 'left'}),
                        use_container_width=True
                    )

            except Exception as e:
                st.error(f"❌ Erreur lors de l'affichage : {e}")

st.markdown("---")
st.markdown(f"📁 **Base utilisée** : `{ACCESS_DB_PATH}`")