import logging
import streamlit as st
import polars as pl

from utils import config
from utils.data_cleaner import short_hash
from utils.ui_utils import get_column_display_name

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(config.APP_TITLE)


def step_concatenate_columns():
    st.subheader("🔗 Объединение колонок")
    df = st.session_state.df

    standalone_columns = [
        col for col in df.columns
        if st.session_state.columns_data[col]["mode"] == "standalone"
    ]

    columns_to_concat = st.multiselect(
        "Выберите колонки для объединения (порядок важен):",
        options=standalone_columns,
        format_func=get_column_display_name
    )

    col1, col2 = st.columns(2)
    with col1:
        concat_name = st.text_input("Название новой колонки:", value="concatenated_column")
    with col2:
        separator = st.text_input("Разделитель:", value="")
    st.markdown(
        "**Примечание:** Если это поле основное, рекомендуем выбрать из списка:\n\n"
        f"`{', '.join(config.DEFAULT_COLUMN_NAMES)}`"
    )
    if columns_to_concat:
        try:
            preview_expr = pl.concat_str(
                [pl.col(col).cast(str).str.strip_chars() for col in columns_to_concat],
                separator=separator
            ).alias(concat_name)

            preview_df = df.select(preview_expr).head(5)
            st.markdown("#### Превью объединённой колонки:")
            st.dataframe(preview_df.to_pandas(), hide_index=True, use_container_width=True)
        except Exception as e:
            logger.warning(f"Failed to generate preview for concatenation: {e}")
            st.warning(f"Ошибка при формировании превью: {e}")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Объединить колонки", disabled=not columns_to_concat):
            handle_concatenate_columns(df, columns_to_concat, concat_name, separator)
    with col2:
        st.checkbox("Удалить исходные колонки", key="delete_original")


def handle_concatenate_columns(df, columns_to_concat, concat_name, separator):
    if concat_name in df.columns:
        st.error(f"Колонка '{concat_name}' уже существует!")
        logger.error(f"Column '{concat_name}' already exists in the dataframe.")
        return

    try:
        logger.info(f"Concatenating columns: {columns_to_concat} into '{concat_name}' with separator '{separator}'")

        all_patterns = []
        all_display_patterns = []

        for col in columns_to_concat:
            col_data = st.session_state.columns_data.get(col, {})
            all_patterns.extend(col_data.get("detected_patterns", []))
            all_display_patterns.extend(col_data.get("detected_display_patterns", []))

        unique_patterns = sorted(set(all_patterns))
        unique_display_patterns = sorted(set(all_display_patterns))

        cleaned_columns = [
            pl.col(col).cast(str).str.strip_chars() for col in columns_to_concat
        ]
        new_column = pl.concat_str(cleaned_columns, separator=separator).alias(concat_name)

        new_df = df.with_columns([new_column])
        st.session_state.df = new_df
        st.session_state.origin_df = new_df

        # Обновляем мета-данные
        st.session_state.columns_data[concat_name] = {
            "hash": short_hash(concat_name),
            "column": concat_name,
            "origin_name": concat_name,
            "display_name": concat_name,
            "detected_patterns": unique_patterns,
            "prev_selected_patterns": None,
            "selected_patterns": unique_patterns,
            "detected_display_patterns": unique_display_patterns,
            "display_patterns": unique_display_patterns,
            "mode": "standalone",
            "concatenated": {
                "source_columns": columns_to_concat,
                "separator": separator
            }
        }

        if st.session_state.get("delete_original"):
            for col in columns_to_concat:
                if col in st.session_state.columns_data:
                    st.session_state.columns_data[col]["mode"] = "exclude"
            logger.info("Original columns marked as excluded.")

        logger.info(f"Columns {columns_to_concat} successfully concatenated into '{concat_name}'")
        st.success(f"✅ Колонки {columns_to_concat} объединены в '{concat_name}'")

    except Exception as e:
        logger.exception("Error while concatenating columns")
        st.error(f"❌ Ошибка при объединении колонок: {e}")
