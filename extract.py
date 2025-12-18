"""
Script autonome d'extraction de transactions bancaires avec LLaMA Parse
en utilisant la méthode Markdown pour plus de fiabilité.
"""

import pandas as pd
import re
import os
import requests
import time
from typing import Optional, Tuple
from dotenv import load_dotenv

def parse_markdown_table(markdown_text: str) -> pd.DataFrame:
    """
    Parse un tableau au format Markdown en DataFrame pandas.
    Cette version isole d'abord le tableau avant de l'analyser.
    """
    md_content = markdown_text
    if '```markdown' in markdown_text:
        match = re.search(r'```markdown\n(.*?)\n```', markdown_text, re.DOTALL)
        if match:
            md_content = match.group(1)

    table_lines = [line.strip() for line in md_content.strip().split('\n') if line.strip().startswith('|')]

    if not table_lines:
        print("⚠️ Aucune ligne de tableau Markdown valide n'a été trouvée dans le texte reçu.")
        return pd.DataFrame()

    header_line = table_lines[0]
    headers = [h.strip() for h in header_line.strip('|').split('|')]
    
    data_lines = []
    for line in table_lines[1:]:
        if '---' not in line:
            data_lines.append(line)

    records = []
    for line in data_lines:
        cells = [c.strip() for c in line.strip('|').split('|')]
        if len(cells) == len(headers):
            records.append(dict(zip(headers, cells)))
        else:
            print(f"⚠️ Ligne ignorée (nombre de cellules incorrect): {line}")

    if not records:
        print("⚠️ Aucune transaction n'a pu être extraite du tableau.")
        return pd.DataFrame()
        
    df = pd.DataFrame(records)
    
    rename_map = {col: col.lower().replace('n° ', '').replace(' ', '_') for col in df.columns}
    df = df.rename(columns=rename_map)

    standard_names = {
        'date': 'date', 'date_valeur': 'date_valeur', 'libellé': 'libelle', 
        'débit': 'debit', 'crédit': 'credit', 'solde': 'solde',
        'chèque': 'cheque', 'porteur': 'porteur'
    }
    df = df.rename(columns=lambda c: standard_names.get(c, c))

    if 'libelle' in df.columns:
        df = df[~df['libelle'].str.contains("Solde précédent", na=False, case=False)]

    print(f"✅ Tableau Markdown parsé. {len(df)} transactions trouvées.")
    return df


def extract_transactions_llama_markdown(pdf_path: str, api_key: str) -> Tuple[pd.DataFrame, Optional[float]]:
    """
    Utilise l'API LLaMA Cloud pour extraire les transactions via le format Markdown.
    """
    if not api_key or not api_key.startswith("llx-"):
        raise ValueError("Clé API LLaMA Cloud manquante ou invalide.")

    upload_url = "https://api.cloud.llamaindex.ai/api/parsing/upload"
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
    
    parsing_instruction = """
    Extrais les transactions de ce relevé bancaire et présente-les sous forme de tableau Markdown.
    Inclure les colonnes: Date, Date Valeur, Libellé, N° Chèque, Porteur, Débit, Crédit, Solde.
    IMPORTANT: Tout le contenu d'une ligne du tableau doit être sur une seule ligne. N'insère JAMAIS de caractère de nouvelle ligne dans une cellule.
    """
    
    print(f"📤 Upload du fichier vers LLaMA Cloud: {pdf_path}")
    with open(pdf_path, 'rb') as f:
        files = {'file': (os.path.basename(pdf_path), f, 'application/pdf')}
        data = {'language': 'fr', 'parsing_instruction': parsing_instruction}
        response = requests.post(upload_url, headers=headers, files=files, data=data)
    response.raise_for_status()
    
    job_id = response.json().get('id')
    print(f"✅ Upload réussi - Job ID: {job_id}. En attente du traitement...")
    
    result_url = f"https://api.cloud.llamaindex.ai/api/parsing/job/{job_id}/result/markdown"
    
    for attempt in range(60):
        time.sleep(2)
        result_response = requests.get(result_url, headers=headers)
        if result_response.status_code == 200:
            extracted_text = result_response.json().get('markdown', '')
            print(f"✅ Extraction Markdown terminée.")
            if not extracted_text:
                raise ValueError("L'extraction a réussi mais le contenu Markdown est vide.")
            
            print("\n--- Début du Markdown Brut Reçu ---")
            print(extracted_text)
            print("--- Fin du Markdown Brut Reçu ---\n")
            
            solde_precedent = None
            match = re.search(r"Solde précédent.*\|([\d\s.,]+)", extracted_text, re.IGNORECASE)
            if match:
                try:
                    solde_str = re.sub(r'[^\d]', '', match.group(1))
                    if solde_str: solde_precedent = float(solde_str)
                except (ValueError, IndexError):
                    print("⚠️ Impossible de parser le solde précédent.")
            
            df = parse_markdown_table(extracted_text)
            return df, solde_precedent
        
        elif result_response.status_code in [404, 400]:
             print(f"⏳ Traitement en cours... (tentative {attempt + 1}/60)")
             continue
        else:
            result_response.raise_for_status()
    else:
        raise TimeoutError("Timeout - Le traitement du document a pris trop de temps.")

def clean_and_format_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie, formate, et applique les transformations demandées au DataFrame."""
    
    if 'porteur' in df.columns and 'libelle' in df.columns:
        df['porteur'] = df['porteur'].fillna('')
        df['libelle'] = df['libelle'].fillna('')
        df['libelle'] = df.apply(lambda row: f"{row['libelle']} - {row['porteur']}" if row['porteur'] else row['libelle'], axis=1)

    cols_to_drop = ['cheque', 'porteur']
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore')

    # Logique de nettoyage des nombres rendue plus robuste
    for col in ['debit', 'credit', 'solde']:
        if col in df.columns:
            # Ne garde que les chiffres, supprime tout le reste.
            df[col] = df[col].astype(str).str.replace(r'[^\d]', '', regex=True)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    for col in ['date', 'date_valeur']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce').dt.date

    if 'date' in df.columns:
        df = df.sort_values('date').reset_index(drop=True)
        
    return df

def analyze_and_export(df: pd.DataFrame, output_prefix: str = "transactions", solde_precedent: Optional[float] = None):
    """Analyse les transactions et exporte les résultats."""
    print("\n" + "=" * 70)
    print("📊 ANALYSE DES TRANSACTIONS")
    print("=" * 70)
    
    if df.empty:
        print("❌ Aucune transaction à analyser")
        return

    if solde_precedent is not None:
        print(f"🏦 Solde précédent: {solde_precedent:,.0f} FCFA")

    print(f"📈 Nombre de transactions: {len(df)}")
    
    if 'debit' in df.columns and df['debit'].notna().any():
        total_debits = df['debit'].sum()
        print(f"💸 Total des débits: {total_debits:,.0f} FCFA")
    
    df_export = df.copy()
    for col in ['date', 'date_valeur']:
        if col in df_export.columns:
            df_export[col] = pd.to_datetime(df_export[col]).dt.strftime('%d/%m/%Y')
        
    csv_file = f"{output_prefix}.csv"
    df_export.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print(f"\n✅ Exporté vers: {csv_file}")
    try:
        excel_file = f"{output_prefix}.xlsx"
        df_export.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"✅ Exporté vers: {excel_file}")
    except ImportError:
        print("\n💡 Pour exporter vers Excel, installez openpyxl")

if __name__ == "__main__":
    load_dotenv() 
    
    API_KEY = os.getenv("LLAMA_CLOUD_API_KEY", "")
    PDF_PATH = r"ocr_split_pages\ocr_page_1.pdf"

    if not API_KEY or not API_KEY.startswith("llx-"):
        print("❌ Clé API LLaMA Cloud manquante ou invalide.")
        exit(1)

    if not os.path.exists(PDF_PATH):
        print(f"❌ Fichier PDF non trouvé: {PDF_PATH}")
        exit(1)

    try:
        print("\n🚀 Lancement de l'extraction (méthode Llama Markdown v4)...")
        print("-" * 70)
        df, solde_precedent = extract_transactions_llama_markdown(PDF_PATH, API_KEY)
        
        if not df.empty:
            df_cleaned = clean_and_format_dataframe(df)
            analyze_and_export(df_cleaned, "transactions_orabank", solde_precedent)
        else:
            print("❌ L'extraction n'a produit aucune donnée.")
        
    except (ImportError, ModuleNotFoundError):
         print(f"\n⚠️ Modules manquants. Installation: pip install pandas openpyxl python-dotenv requests")
    except Exception as e:
        print(f"\n❌ Une erreur majeure est survenue: {e}")
        import traceback
        traceback.print_exc()