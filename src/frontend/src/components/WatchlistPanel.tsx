import type { WatchlistItem } from "../types";

type WatchlistPanelProps = {
  items: WatchlistItem[];
  selectedCode: string | null;
  isUpdating: boolean;
  onSelect: (code: string) => void;
  onRemove: (code: string) => Promise<void>;
};

function formatPrice(value: number | null | undefined) {
  if (value == null) {
    return "--";
  }
  return value.toFixed(2);
}

export function WatchlistPanel({
  items,
  selectedCode,
  isUpdating,
  onSelect,
  onRemove
}: WatchlistPanelProps) {
  return (
    <section className="panel watchlist-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">观察池</p>
          <h2>跟踪中的股票</h2>
        </div>
        <span className="table-count">{items.length} 只</span>
      </div>

      <div className="watchlist-list">
        {items.map((item) => (
          <div
            key={item.code}
            className={`watchlist-card ${selectedCode === item.code ? "selected" : ""}`}
            onClick={() => onSelect(item.code)}
          >
            <div className="watchlist-head">
              <div>
                <strong>
                  {item.name || item.code} <span>{item.code}</span>
                </strong>
                <p>{item.note || "暂无备注"}</p>
              </div>
              <button
                type="button"
                className="ghost-button"
                disabled={isUpdating}
                onClick={(event) => {
                  event.stopPropagation();
                  void onRemove(item.code);
                }}
              >
                移除
              </button>
            </div>
            <div className="watchlist-meta">
              <span>现价 {formatPrice(item.last_price)}</span>
              <span>目标 {formatPrice(item.target_entry)}</span>
              <span>止损 {formatPrice(item.stop_loss)}</span>
              <span>止盈 {formatPrice(item.take_profit)}</span>
            </div>
          </div>
        ))}

        {items.length === 0 ? <div className="empty-card">从单股详情页保存后，观察池会出现在这里。</div> : null}
      </div>
    </section>
  );
}
