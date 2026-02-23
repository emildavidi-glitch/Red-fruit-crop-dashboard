import { useState, useEffect, useCallback, useRef } from "react";

const REGIONS = [
  { code: "US", name: "United States", flag: "🇺🇸", currency: "$", marketEst: "~$265B", growthEst: "~+3%" },
  { code: "DE", name: "Germany", flag: "🇩🇪", currency: "€", marketEst: "~€29B", growthEst: "~+2%" },
  { code: "FR", name: "France", flag: "🇫🇷", currency: "€", marketEst: "~€22B", growthEst: "~+2%" },
  { code: "ES", name: "Spain", flag: "🇪🇸", currency: "€", marketEst: "~€12B", growthEst: "~+3%" },
  { code: "IT", name: "Italy", flag: "🇮🇹", currency: "€", marketEst: "~€18B", growthEst: "~+3%" },
  { code: "AT", name: "Austria", flag: "🇦🇹", currency: "€", marketEst: "~€5B", growthEst: "~+2%" },
];

const TODAY = new Date().toLocaleDateString("en-GB", {
  weekday: "long",
  day: "numeric",
  month: "long",
  year: "numeric",
}).toUpperCase();

async function fetchInsight(prompt) {
  try {
    const res = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 1000,
        system: `You are a beverage industry market analyst. You provide very short, factual, current market insights. Always respond ONLY with valid JSON, no markdown, no backticks, no preamble. Today's date is ${new Date().toISOString().split("T")[0]}.`,
        messages: [{ role: "user", content: prompt }],
        tools: [{ type: "web_search_20250305", name: "web_search" }],
      }),
    });
    const data = await res.json();
    const text = data.content
      ?.map((b) => (b.type === "text" ? b.text : ""))
      .filter(Boolean)
      .join("\n");
    const clean = text.replace(/```json|```/g, "").trim();
    return JSON.parse(clean);
  } catch (e) {
    console.error("API error:", e);
    return null;
  }
}

function StatusDot({ status }) {
  const colors = {
    loading: "#f59e0b",
    done: "#10b981",
    error: "#ef4444",
    idle: "#64748b",
  };
  return (
    <span
      style={{
        display: "inline-block",
        width: 8,
        height: 8,
        borderRadius: "50%",
        background: colors[status] || colors.idle,
        marginLeft: 6,
        animation: status === "loading" ? "pulse 1.2s infinite" : "none",
      }}
    />
  );
}

function RegionCard({ region, insight, status, isSelected, onClick }) {
  const getIcon = (sentiment) => {
    if (sentiment === "positive") return "🔥";
    if (sentiment === "neutral") return "⚡";
    if (sentiment === "negative") return "⚠️";
    return "📊";
  };

  const getSentimentColor = (sentiment) => {
    if (sentiment === "positive") return "#059669";
    if (sentiment === "neutral") return "#d97706";
    if (sentiment === "negative") return "#dc2626";
    return "#64748b";
  };

  return (
    <div
      onClick={onClick}
      style={{
        background: isSelected ? "#f8fafc" : "#ffffff",
        border: isSelected ? "2px solid #1e3a5f" : "1.5px solid #e2e8f0",
        borderRadius: 12,
        padding: "20px 22px",
        cursor: "pointer",
        transition: "all 0.2s ease",
        position: "relative",
        minHeight: 170,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
        <span style={{ fontSize: 13, fontWeight: 700, color: "#94a3b8", fontFamily: "'JetBrains Mono', monospace", letterSpacing: 1 }}>
          {region.code}
        </span>
        <span style={{ fontSize: 17, fontWeight: 600, color: "#0f172a", fontFamily: "'DM Sans', sans-serif" }}>
          {region.name}
        </span>
        <StatusDot status={status} />
      </div>

      <div style={{ display: "flex", gap: 16, marginBottom: 14 }}>
        <div>
          <div style={{ fontSize: 20, fontWeight: 700, color: "#0f172a", fontFamily: "'DM Sans', sans-serif" }}>
            {insight?.market_size || region.marketEst}
          </div>
          <div style={{ fontSize: 10, fontWeight: 600, color: "#94a3b8", letterSpacing: 1.5, textTransform: "uppercase", fontFamily: "'JetBrains Mono', monospace" }}>
            Market (Est.)
          </div>
        </div>
        <div>
          <div style={{ fontSize: 20, fontWeight: 700, color: "#059669", fontFamily: "'DM Sans', sans-serif" }}>
            {insight?.growth || region.growthEst}
          </div>
          <div style={{ fontSize: 10, fontWeight: 600, color: "#94a3b8", letterSpacing: 1.5, textTransform: "uppercase", fontFamily: "'JetBrains Mono', monospace" }}>
            Growth (Est.)
          </div>
        </div>
      </div>

      {status === "loading" && (
        <div style={{
          background: "#f1f5f9",
          borderRadius: 8,
          padding: "10px 14px",
          display: "flex",
          alignItems: "center",
          gap: 8,
        }}>
          <div className="shimmer" style={{ height: 12, borderRadius: 4, flex: 1 }} />
        </div>
      )}

      {status === "done" && insight?.remark && (
        <div style={{
          background: `${getSentimentColor(insight.sentiment)}11`,
          borderRadius: 8,
          padding: "10px 14px",
          display: "flex",
          alignItems: "center",
          gap: 8,
          borderLeft: `3px solid ${getSentimentColor(insight.sentiment)}`,
        }}>
          <span style={{ fontSize: 14 }}>{getIcon(insight.sentiment)}</span>
          <span style={{
            fontSize: 13,
            color: "#334155",
            fontFamily: "'JetBrains Mono', monospace",
            lineHeight: 1.4,
            fontWeight: 500,
          }}>
            {insight.remark}
          </span>
        </div>
      )}

      {status === "error" && (
        <div style={{
          background: "#fef2f2",
          borderRadius: 8,
          padding: "10px 14px",
          fontSize: 12,
          color: "#dc2626",
          fontFamily: "'JetBrains Mono', monospace",
        }}>
          Failed to fetch insight. Click to retry.
        </div>
      )}
    </div>
  );
}

function MorningBriefing({ briefing, status }) {
  return (
    <div style={{
      background: "linear-gradient(135deg, #0f2027 0%, #1a3a4a 50%, #203a43 100%)",
      borderRadius: 14,
      padding: "22px 28px",
      marginBottom: 28,
      position: "relative",
      overflow: "hidden",
    }}>
      <div style={{
        position: "absolute",
        top: 16,
        right: 20,
        width: 50,
        height: 50,
        borderRadius: "50%",
        background: "rgba(255,255,255,0.06)",
      }} />

      <div style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        marginBottom: 12,
      }}>
        <span style={{ fontSize: 14 }}>📰</span>
        <span style={{
          fontSize: 11,
          fontWeight: 700,
          color: "#94a3b8",
          letterSpacing: 2,
          textTransform: "uppercase",
          fontFamily: "'JetBrains Mono', monospace",
        }}>
          MORNING BRIEFING · {TODAY}
        </span>
        <StatusDot status={status} />
      </div>

      {status === "loading" && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          <div className="shimmer" style={{ height: 14, borderRadius: 4, width: "100%" }} />
          <div className="shimmer" style={{ height: 14, borderRadius: 4, width: "90%" }} />
          <div className="shimmer" style={{ height: 14, borderRadius: 4, width: "70%" }} />
        </div>
      )}

      {status === "done" && briefing && (
        <>
          <p style={{
            fontSize: 14.5,
            color: "#e2e8f0",
            lineHeight: 1.65,
            margin: "0 0 12px 0",
            fontFamily: "'DM Sans', sans-serif",
            maxWidth: 900,
          }}>
            {briefing.summary}
          </p>
          <div style={{
            fontSize: 11,
            color: "#64748b",
            fontFamily: "'JetBrains Mono', monospace",
          }}>
            Sources: Web search via Claude API — Updated on demand
          </div>
        </>
      )}

      {status === "error" && (
        <p style={{ fontSize: 13, color: "#f87171", fontFamily: "'DM Sans', sans-serif" }}>
          Could not generate briefing. Click "Refresh All" to retry.
        </p>
      )}
    </div>
  );
}

export default function BeverageDashboard() {
  const [activeTab, setActiveTab] = useState("global");
  const [briefing, setBriefing] = useState(null);
  const [briefingStatus, setBriefingStatus] = useState("idle");
  const [insights, setInsights] = useState({});
  const [regionStatus, setRegionStatus] = useState({});
  const [lastRefresh, setLastRefresh] = useState(null);
  const fetchingRef = useRef(false);

  const fetchBriefing = useCallback(async () => {
    setBriefingStatus("loading");
    const result = await fetchInsight(
      `Search the web for the latest functional beverage and soft drinks market news from the past 2 weeks across the US and Europe. Then return a JSON object with this exact structure: {"summary": "<A 2-3 sentence market briefing covering the most important current trends, regulatory changes, commodity prices, or notable product launches. Be specific with names, numbers, dates where possible. Do NOT invent data.>"}`
    );
    if (result?.summary) {
      setBriefing(result);
      setBriefingStatus("done");
    } else {
      setBriefingStatus("error");
    }
  }, []);

  const fetchRegionInsight = useCallback(async (regionCode) => {
    setRegionStatus((prev) => ({ ...prev, [regionCode]: "loading" }));
    const region = REGIONS.find((r) => r.code === regionCode);
    const result = await fetchInsight(
      `Search the web for the latest beverage market trends in ${region.name} from the past month. Focus on functional beverages, energy drinks, soft drinks, juice, sparkling water. Then return a JSON object with this exact structure: {"remark": "<One concise sentence about the most notable current trend or development. Be specific, cite a brand, product, regulation, or data point if found.>", "sentiment": "<positive|neutral|negative>", "market_size": "<estimated total non-alcoholic beverage market size for ${region.name}, formatted like ~$265B or ~€29B>", "growth": "<estimated YoY growth rate like ~+3%>"}`
    );
    if (result?.remark) {
      setInsights((prev) => ({ ...prev, [regionCode]: result }));
      setRegionStatus((prev) => ({ ...prev, [regionCode]: "done" }));
    } else {
      setRegionStatus((prev) => ({ ...prev, [regionCode]: "error" }));
    }
  }, []);

  const fetchAll = useCallback(async () => {
    if (fetchingRef.current) return;
    fetchingRef.current = true;
    
    // Fetch briefing first
    await fetchBriefing();
    
    // Then fetch region insights (staggered to avoid rate limits)
    for (const region of REGIONS) {
      await fetchRegionInsight(region.code);
      // Small delay between requests
      await new Promise((r) => setTimeout(r, 800));
    }
    
    setLastRefresh(new Date());
    fetchingRef.current = false;
  }, [fetchBriefing, fetchRegionInsight]);

  useEffect(() => {
    fetchAll();
  }, []);

  const allLoading = briefingStatus === "loading" || Object.values(regionStatus).some((s) => s === "loading");

  return (
    <div style={{
      minHeight: "100vh",
      background: "#f1f5f9",
      fontFamily: "'DM Sans', sans-serif",
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap');
        
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.3; }
        }
        
        @keyframes shimmer {
          0% { background-position: -200px 0; }
          100% { background-position: 200px 0; }
        }
        
        .shimmer {
          background: linear-gradient(90deg, rgba(255,255,255,0.05) 25%, rgba(255,255,255,0.15) 50%, rgba(255,255,255,0.05) 75%);
          background-size: 400px 100%;
          animation: shimmer 1.5s infinite;
        }

        .tab-btn {
          padding: 8px 16px;
          border: none;
          background: transparent;
          font-size: 13px;
          font-weight: 600;
          color: #64748b;
          cursor: pointer;
          border-radius: 8px;
          font-family: 'DM Sans', sans-serif;
          display: flex;
          align-items: center;
          gap: 6px;
          transition: all 0.15s;
        }
        .tab-btn:hover { background: #e2e8f0; color: #1e293b; }
        .tab-btn.active { background: #1e3a5f; color: #fff; }

        .refresh-btn {
          padding: 8px 18px;
          border: 1.5px solid #cbd5e1;
          background: #fff;
          font-size: 12px;
          font-weight: 600;
          color: #334155;
          cursor: pointer;
          border-radius: 8px;
          font-family: 'JetBrains Mono', monospace;
          display: flex;
          align-items: center;
          gap: 6px;
          transition: all 0.15s;
          letter-spacing: 0.5px;
        }
        .refresh-btn:hover { background: #f8fafc; border-color: #94a3b8; }
        .refresh-btn:disabled { opacity: 0.5; cursor: not-allowed; }
      `}</style>

      <div style={{ maxWidth: 1100, margin: "0 auto", padding: "28px 24px" }}>
        {/* Header */}
        <div style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: 24,
          flexWrap: "wrap",
          gap: 12,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
            <button
              className={`tab-btn ${activeTab === "global" ? "active" : ""}`}
              onClick={() => setActiveTab("global")}
            >
              🌍 Global Overview
            </button>
            {REGIONS.map((r) => (
              <button
                key={r.code}
                className={`tab-btn ${activeTab === r.code ? "active" : ""}`}
                onClick={() => setActiveTab(r.code)}
              >
                {r.code} {r.name}
                <StatusDot status={regionStatus[r.code] || "idle"} />
              </button>
            ))}
          </div>
          <button
            className="refresh-btn"
            onClick={fetchAll}
            disabled={allLoading}
          >
            {allLoading ? "⏳ Fetching…" : "🔄 Refresh All"}
          </button>
        </div>

        {/* Morning Briefing */}
        <MorningBriefing briefing={briefing} status={briefingStatus} />

        {/* Region Cards Grid */}
        {activeTab === "global" ? (
          <div style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(310, 1fr))",
            gap: 18,
          }}>
            <div style={{
              display: "grid",
              gridTemplateColumns: "repeat(3, 1fr)",
              gap: 18,
            }}>
              {REGIONS.map((region) => (
                <RegionCard
                  key={region.code}
                  region={region}
                  insight={insights[region.code]}
                  status={regionStatus[region.code] || "idle"}
                  isSelected={false}
                  onClick={() => {
                    if (regionStatus[region.code] === "error") {
                      fetchRegionInsight(region.code);
                    }
                    setActiveTab(region.code);
                  }}
                />
              ))}
            </div>
          </div>
        ) : (
          <div>
            {(() => {
              const region = REGIONS.find((r) => r.code === activeTab);
              if (!region) return null;
              const insight = insights[region.code];
              const status = regionStatus[region.code] || "idle";
              return (
                <div style={{ maxWidth: 600 }}>
                  <RegionCard
                    region={region}
                    insight={insight}
                    status={status}
                    isSelected={true}
                    onClick={() => {
                      if (status === "error") fetchRegionInsight(region.code);
                    }}
                  />
                  {status === "done" && insight && (
                    <div style={{
                      marginTop: 18,
                      padding: "18px 22px",
                      background: "#fff",
                      borderRadius: 12,
                      border: "1.5px solid #e2e8f0",
                    }}>
                      <div style={{
                        fontSize: 11,
                        fontWeight: 700,
                        color: "#94a3b8",
                        letterSpacing: 2,
                        marginBottom: 10,
                        fontFamily: "'JetBrains Mono', monospace",
                      }}>
                        DETAILED INSIGHT
                      </div>
                      <p style={{
                        fontSize: 14,
                        color: "#334155",
                        lineHeight: 1.6,
                        margin: 0,
                      }}>
                        {insight.remark}
                      </p>
                      <div style={{
                        marginTop: 14,
                        fontSize: 11,
                        color: "#94a3b8",
                        fontFamily: "'JetBrains Mono', monospace",
                      }}>
                        Market: {insight.market_size} · Growth: {insight.growth} · Sentiment: {insight.sentiment}
                      </div>
                    </div>
                  )}
                  <button
                    onClick={() => setActiveTab("global")}
                    style={{
                      marginTop: 16,
                      padding: "8px 16px",
                      background: "transparent",
                      border: "1px solid #cbd5e1",
                      borderRadius: 8,
                      fontSize: 12,
                      color: "#64748b",
                      cursor: "pointer",
                      fontFamily: "'JetBrains Mono', monospace",
                    }}
                  >
                    ← Back to Global Overview
                  </button>
                </div>
              );
            })()}
          </div>
        )}

        {/* Footer */}
        <div style={{
          marginTop: 32,
          padding: "14px 0",
          borderTop: "1px solid #e2e8f0",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          flexWrap: "wrap",
          gap: 8,
        }}>
          <span style={{
            fontSize: 11,
            color: "#94a3b8",
            fontFamily: "'JetBrains Mono', monospace",
          }}>
            Powered by Claude API with web search · All insights are AI-generated from current web sources
          </span>
          {lastRefresh && (
            <span style={{
              fontSize: 11,
              color: "#94a3b8",
              fontFamily: "'JetBrains Mono', monospace",
            }}>
              Last refresh: {lastRefresh.toLocaleTimeString()}
            </span>
          )}
        </div>
      </div>
    </div>
  );
}
