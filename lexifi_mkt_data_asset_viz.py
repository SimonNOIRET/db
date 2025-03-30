import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from io import BytesIO
from sqlalchemy import create_engine
import base64

# ----------------------- CONFIG BDD -----------------------
DB_USER = "postgres"
DB_PASSWORD = "0112"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "lexifi_mkt_data"

TABLE_NAME = "asset_spot"
ID_COL = "lexifi_id"
DATE_COL = "lexifi_date"
VALUE_COL = "lexifi_spot"

FORWARD_TABLE = "asset_fwd"
FORWARD_VALUE = "lexifi_forward"
FORWARD_ID = "lexifi_forward_id"
FORWARD_DATE = "lexifi_date"
FORWARD_BASE_ID = "lexifi_id"

# ----------------------- FONCTIONS -----------------------

def get_engine():
    engine_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(engine_str)

@st.cache_data
def connect_and_fetch_ids():
    engine = get_engine()
    df = pd.read_sql(f"SELECT DISTINCT {ID_COL} FROM {TABLE_NAME}", con=engine)
    return df[ID_COL].dropna().astype(str).tolist()

@st.cache_data
def fetch_data_for_id(selected_id):
    engine = get_engine()
    query = f"""
        SELECT {DATE_COL}, {VALUE_COL}
        FROM {TABLE_NAME}
        WHERE {ID_COL} = %s
        ORDER BY {DATE_COL}
    """
    df = pd.read_sql(query, con=engine, params=(selected_id,))
    df[DATE_COL] = pd.to_datetime(df[DATE_COL])
    df["id"] = selected_id
    return df

@st.cache_data
def fetch_asset_mapping():
    engine = get_engine()
    df = pd.read_sql("SELECT lexifi_id, asset_name FROM asset_mapping", con=engine)
    df['asset_name'] = df['asset_name'].astype(str).apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8'))
    return dict(zip(df["lexifi_id"], df["asset_name"]))

@st.cache_data
def fetch_forward_ids():
    engine = get_engine()
    df = pd.read_sql(f"""
        SELECT DISTINCT {FORWARD_ID}, {FORWARD_BASE_ID}
        FROM {FORWARD_TABLE}
        WHERE {FORWARD_ID} IS NOT NULL AND {FORWARD_VALUE} IS NOT NULL
    """, con=engine)
    return df

# ----------------------- PAGE CONFIG -----------------------
st.set_page_config(
    page_title="Arkea Asset Management",
    page_icon="C:/Users/Simon/Documents/ArkeaAM/VSCode/icons/AAM_1.png",
    layout="wide"
)

st.title("🔍 Market Data Overwatch 🔍")
st.caption("Source: LexiFi")

asset_name_map = fetch_asset_mapping()
id_list = connect_and_fetch_ids()
display_list = [f"{id_} - {asset_name_map.get(id_, 'Inconnu')}" for id_ in id_list]
id_display_map = dict(zip(display_list, id_list))

tab_labels = ["📈 Spot", "📈 Forward"]
tabs = st.tabs(tab_labels)

# ----------------------- ONGLET SPOT -----------------------
with tabs[0]:
    selected_display = st.multiselect("Sélectionner un ou plusieurs IDs :", display_list)
    selected_ids = [id_display_map[label] for label in selected_display]

    if selected_ids:
        all_data = [fetch_data_for_id(i) for i in selected_ids]
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
                            rebased_df.loc[first_valid:, col] = (rebased_df[col] / base_value) * 100
                            rebased_df.loc[:first_valid, col] = 100
                        else:
                            excluded_series.append(col)

                    if excluded_series:
                        st.info(f"Séries exclues : {', '.join(excluded_series)}")
                        rebased_df.drop(columns=excluded_series, inplace=True)

                    plot_df = rebased_df.reset_index().melt(id_vars=DATE_COL, var_name="lexifi_id", value_name="Valeur")
                else:
                    plot_df = filtered_df.reset_index().melt(id_vars=DATE_COL, var_name="lexifi_id", value_name="Valeur")

                plot_df["Asset"] = plot_df["lexifi_id"].map(asset_name_map).fillna(plot_df["lexifi_id"])
                chart_title = f"Séries rebasées à 100 à partir du {start_date}" if len(selected_ids) > 1 else f"Évolution historique de l'actif : {plot_df['Asset'].iloc[0]}"

                fig = px.line(
                    plot_df,
                    x=DATE_COL,
                    y="Valeur",
                    color="Asset",
                    title=chart_title
                )
                fig.update_layout(height=800, xaxis_title="Date", yaxis_title="Valeur", hovermode="x unified")
                st.plotly_chart(fig, use_container_width=True)

                raw_plot_df = filtered_df.reset_index().melt(id_vars=DATE_COL, var_name="ID", value_name="Valeur")
                csv_buffer = BytesIO()
                raw_plot_df.to_csv(csv_buffer, index=False)
                st.download_button("📁 Télécharger les données affichées", data=csv_buffer.getvalue(), file_name="donnees.csv", mime="text/csv")

                st.subheader("📊 Statistiques")

                stats_summary = []
                last_date = filtered_df.index.max()
                start_year = pd.to_datetime(start_date).year
                years = list(range(start_year, last_date.year + 1))

                for col in filtered_df.columns:
                    serie = filtered_df[col].dropna()
                    if serie.empty:
                        continue

                    val_current = serie.iloc[-1]
                    val_min = serie.min()
                    val_max = serie.max()
                    returns = serie.pct_change().dropna()
                    vol_annuelle = returns.std() * np.sqrt(252)

                    perf_by_year = {}
                    for year in years:
                        try:
                            dec_31 = pd.Timestamp(f"{year-1}-12-31")
                            end_val = serie[serie.index.year == year].iloc[-1]
                            start_val = serie[serie.index <= dec_31].iloc[-1]
                            perf_by_year[f"Perf {year}"] = (end_val / start_val - 1) * 100
                        except:
                            perf_by_year[f"Perf {year}"] = None

                    try:
                        ytd_start_val = serie[serie.index <= pd.Timestamp(f"{last_date.year - 1}-12-31")].iloc[-1]
                        perf_ytd = (val_current / ytd_start_val - 1) * 100
                    except:
                        perf_ytd = None

                    stats_summary.append({
                        "Actif": asset_name_map.get(col, col),
                        "Valeur actuelle": val_current,
                        "Min": val_min,
                        "Max": val_max,
                        "Perf YTD": perf_ytd,
                        "Volatilité réalisée (%)": vol_annuelle * 100,
                        **perf_by_year
                    })

                stats_table = pd.DataFrame(stats_summary)

                def format_number(x):
                    try:
                        return f"{float(x):,.2f}".replace(",", " ").replace(".00", ".00")
                    except:
                        return "-"

                def format_percent(x):
                    try:
                        return f"{float(x):.2f}%"
                    except:
                        return "-"

                for col in stats_table.columns:
                    if col.startswith("Perf") or col in ["Valeur actuelle", "Min", "Max", "Volatilité réalisée (%)"]:
                        stats_table[col] = pd.to_numeric(stats_table[col], errors='coerce')

                perf_cols = [col for col in stats_table.columns if "Perf" in col]

                format_dict = {col: format_percent for col in perf_cols}
                format_dict.update({
                    "Valeur actuelle": format_number,
                    "Min": format_number,
                    "Max": format_number,
                    "Volatilité réalisée (%)": format_percent
                })

                styled = stats_table.style.format(format_dict)

                def color_perf(val):
                    if pd.isna(val):
                        return ""
                    elif val > 0:
                        return "color: green"
                    elif val < 0:
                        return "color: red"
                    return ""

                for col in perf_cols:
                    styled = styled.applymap(color_perf, subset=[col])

                st.dataframe(styled, use_container_width=True)

            except Exception as e:
                st.error(f"Erreur : {e}")

# ----------------------- ONGLET FORWARD -----------------------
with tabs[1]:
    forward_ids_df = fetch_forward_ids()
    forward_ids_df["asset_name"] = forward_ids_df[FORWARD_BASE_ID].map(asset_name_map)
    forward_ids_df["display"] = forward_ids_df[FORWARD_ID] + " - " + forward_ids_df["asset_name"].fillna("Inconnu")
    forward_display_map = dict(zip(forward_ids_df["display"], forward_ids_df[FORWARD_ID]))
    forward_baseid_map = dict(zip(forward_ids_df[FORWARD_ID], forward_ids_df[FORWARD_BASE_ID]))

    selected_display = st.multiselect("Sélectionner un ou plusieurs IDs :", forward_display_map.keys())
    selected_forward_ids = [forward_display_map[d] for d in selected_display]
    selected_base_ids = list(set(forward_baseid_map[fid] for fid in selected_forward_ids))

    if selected_forward_ids:
        engine = get_engine()

        forward_id_tuple = tuple(selected_forward_ids)
        if len(forward_id_tuple) == 1:
            forward_id_tuple += ("",)

        fwd_query = f"""
            SELECT {FORWARD_ID}, {FORWARD_DATE}, {FORWARD_VALUE}, {FORWARD_BASE_ID}
            FROM {FORWARD_TABLE}
            WHERE {FORWARD_ID} IN %s
        """
        fwd_df = pd.read_sql(fwd_query, con=engine, params=(forward_id_tuple,))
        fwd_df[FORWARD_DATE] = pd.to_datetime(fwd_df[FORWARD_DATE])

        if fwd_df.empty:
            st.warning("⚠️ Aucune donnée forward trouvée.")
            st.stop()

        spot_df_all = pd.concat([fetch_data_for_id(i) for i in selected_base_ids])

        merged = []
        for fid in selected_forward_ids:
            base_id = forward_baseid_map[fid]
            fwd_data = fwd_df[fwd_df[FORWARD_ID] == fid][[FORWARD_DATE, FORWARD_VALUE]].rename(columns={FORWARD_DATE: 'date', FORWARD_VALUE: 'fwd'})
            spot_data = spot_df_all[spot_df_all["id"] == base_id][[DATE_COL, VALUE_COL]].rename(columns={DATE_COL: 'date', VALUE_COL: 'spot'})

            merged_df = pd.merge(fwd_data, spot_data, on='date', how='left').sort_values('date')
            if merged_df.empty:
                continue

            merged_df['fwd_spot'] = merged_df['fwd'] / merged_df['spot']
            merged_df['fwd_spot'] = merged_df['fwd_spot'].ffill()
            asset_name = asset_name_map.get(base_id, base_id)
            merged_df['Asset'] = f"{fid} - {asset_name}"
            merged.append(merged_df)

        if not merged:
            st.warning("⚠️ Aucune donnée mergeable Spot/Forward pour les actifs sélectionnés.")
            st.stop()

        ratio_df = pd.concat(merged)
        min_date = ratio_df['date'].min().date()
        max_date = ratio_df['date'].max().date()

        start_date_fwd = st.date_input("Choisir une date de départ pour l’affichage :", value=min_date, min_value=min_date, max_value=max_date, key="start_date_forward")
        plot_df = ratio_df[ratio_df['date'] >= pd.to_datetime(start_date_fwd)]

        fig = px.line(
            plot_df,
            x="date",
            y="fwd_spot",
            color="Asset",
            title=f"Forward en %spot à partir du {start_date_fwd}"
        )
        fig.update_layout(
            height=700,
            xaxis_title="Date",
            yaxis_title="Forward (%Spot)",
            hovermode="x unified"
        )
        fig.update_yaxes(tickformat=".2%")
        st.plotly_chart(fig, use_container_width=True)

    # ----- STRUCTURE PAR TERME -----
    st.markdown("---")
    st.subheader("📉 Structure par terme")

    if selected_forward_ids:
        default_asset_id = forward_baseid_map[selected_forward_ids[0]]

        engine = get_engine()
        date_query = f"""
            SELECT DISTINCT {FORWARD_DATE}
            FROM {FORWARD_TABLE}
            WHERE {FORWARD_BASE_ID} = %s
            ORDER BY {FORWARD_DATE} DESC
        """
        date_df = pd.read_sql(date_query, con=engine, params=(default_asset_id,))
        date_df[FORWARD_DATE] = pd.to_datetime(date_df[FORWARD_DATE])

        if date_df.empty:
            st.warning("⚠️ Aucune date disponible pour cet actif.")
            st.stop()

        selected_term_date = st.date_input(
            "Date d'observation :",
            value=date_df[FORWARD_DATE].max().date(),
            min_value=date_df[FORWARD_DATE].min().date(),
            max_value=date_df[FORWARD_DATE].max().date(),
            key="structure_term_date"
        )

        fwd_term_query = f"""
            SELECT {FORWARD_ID}, {FORWARD_VALUE}
            FROM {FORWARD_TABLE}
            WHERE {FORWARD_BASE_ID} = %s AND {FORWARD_DATE} = %s
        """
        term_df = pd.read_sql(fwd_term_query, con=engine, params=(default_asset_id, selected_term_date))
        if term_df.empty:
            st.warning("⚠️ Aucune donnée forward à cette date pour cet actif.")
            st.stop()

        def extract_years(tenor_str):
            try:
                t = tenor_str.split()[1]
                return int(t.replace("Y", "")) if "Y" in t else None
            except:
                return None

        term_df["Tenor"] = term_df[FORWARD_ID].apply(lambda x: x.split()[1] if len(x.split()) > 1 and "Y" in x.split()[1] else None)
        term_df["Tenor_num"] = term_df["Tenor"].apply(lambda x: int(x.replace("Y", "")) if x else None)
        term_df = term_df.dropna(subset=["Tenor_num"])
        term_df = term_df.sort_values("Tenor_num")

        spot_query = f"""
            SELECT {VALUE_COL}
            FROM {TABLE_NAME}
            WHERE {ID_COL} = %s AND {DATE_COL} = %s
            LIMIT 1
        """
        spot_result = pd.read_sql(spot_query, con=engine, params=(default_asset_id, selected_term_date))

        if not spot_result.empty:
            spot_value = spot_result.iloc[0][VALUE_COL]
            st.markdown(f"📌 **Prix spot au {selected_term_date} : {f'{spot_value:,.2f}'.replace(',', ' ')}**")
        else:
            st.warning(f"Aucun prix spot trouvé au {selected_term_date} pour l'actif sélectionné.")

        fig_term = px.line(
            term_df,
            x="Tenor",
            y=FORWARD_VALUE,
            title=f"Structure par terme • {asset_name_map.get(default_asset_id, default_asset_id)} • {selected_term_date}"
        )
        fig_term.update_layout(
            height=500,
            xaxis_title="Échéance",
            yaxis_title="Forward"
        )
        fig_term.update_yaxes(tickformat=".2f")
        st.plotly_chart(fig_term, use_container_width=True)

    else:
        st.info("Veuillez d'abord sélectionner un ou plusieurs forwards au-dessus pour activer la structure par terme.")

    # ----- PENTES RELATIVES -----
    try:
        forward_series = term_df.set_index("Tenor_num")[FORWARD_VALUE]

        tenors = sorted(forward_series.index.tolist())
        rel_matrix = pd.DataFrame(index=tenors, columns=tenors, dtype=float)

        for i in tenors:
            for j in tenors:
                if forward_series[i] != 0:
                    rel_matrix.loc[i, j] = ((forward_series[j] / forward_series[i]) - 1) * 100
                else:
                    rel_matrix.loc[i, j] = None

        rel_matrix.index = [f"{i}Y" for i in rel_matrix.index]
        rel_matrix.columns = [f"{j}Y" for j in rel_matrix.columns]

        import plotly.figure_factory as ff

        z = rel_matrix.values
        x = rel_matrix.columns.tolist()
        y = rel_matrix.index.tolist()

        fig_rel_heatmap = ff.create_annotated_heatmap(
            z,
            x=x,
            y=y,
            colorscale="RdBu",
            showscale=True,
            reversescale=True,
            zmin=-np.nanmax(np.abs(z)),
            zmax=np.nanmax(np.abs(z)),
            annotation_text=[[f"{v:.2f}%" if pd.notna(v) else "" for v in row] for row in z],
            hoverinfo="z"
        )

        fig_rel_heatmap.update_layout(
            title=f"Matrice des pentes relatives • Fwd(j) / Fwd(i) - 1 • {asset_name_map.get(default_asset_id, default_asset_id)}",
            xaxis_title="Tenor(j)",
            yaxis_title="Tenor(i)",
            height=600,
            margin=dict(l=60, r=60, t=80, b=40)
        )

        st.plotly_chart(fig_rel_heatmap, use_container_width=True)

    except Exception as e:
        st.warning(f"Erreur lors de la génération de la matrice des pentes relatives : {e}")

# ----------------------- FOOTER -----------------------
with open("C:/Users/Simon/Documents/ArkeaAM/VSCode/icons/AAM_2.png", "rb") as f:
    img_bytes = f.read()
    encoded = base64.b64encode(img_bytes).decode()

st.markdown(
    f"""
    <div style='text-align: right; margin-top: 3em;'>
        <span style='font-size: 0.9em; color: gray;'>Simon NOIRET<br>Arkea Asset Management</span><br>
        <img src="data:image/png;base64,{encoded}" width="220">
    </div>
    """,
    unsafe_allow_html=True
)

st.markdown("---")
st.markdown(f"🧩 **Base PostgreSQL utilisée** : `{DB_NAME}` sur `{DB_HOST}:{DB_PORT}`")
