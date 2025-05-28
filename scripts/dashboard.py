import sqlite3

import pandas as pd
import streamlit as st

st.title("📊 游戏行为分析仪表板")

conn = sqlite3.connect("game_events.db")

# 读取数据
df = pd.read_sql("SELECT * FROM GameEvents", conn)
df['EventTimestamp'] = pd.to_datetime(df['EventTimestamp'])
df['date'] = df['EventTimestamp'].dt.date

# DAU
dau = df.groupby('date')['PlayerID'].nunique()
st.line_chart(dau, use_container_width=True)

# 会话时长模拟（基于同一个玩家的 SessionStart → SessionEnd）
session_df = df[df['EventType'].isin(['SessionStart', 'SessionEnd'])]
session_df = session_df.sort_values(['PlayerID', 'EventTimestamp'])

# 应用内收入（用 EventDetails 模拟金额）
iap_df = df[df['EventType'] == 'InAppPurchase']
iap_df.loc[:, 'revenue'] = iap_df['EventDetails'].str.extract(r'Amount:\s*(\d+\.?\d*)')[0].astype(float)
st.metric("📱 总收入", f"${iap_df['revenue'].sum():,.2f}")

# 社交互动次数 / 会话（粗略估算）
social_count = df[df['EventType'] == 'SocialInteraction'].shape[0]
session_count = df[df['EventType'] == 'SessionStart'].shape[0]
social_per_session = round(social_count / session_count, 2)
st.metric("🤝 每次会话的社交互动", social_per_session)

conn.close()
