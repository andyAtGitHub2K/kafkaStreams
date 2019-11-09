package ahbun.model;

import java.time.Instant;
import java.util.Objects;

public class ClickEvent {
    private String symbol;
    private Instant clickInstance;
    private String pageLink;

    public ClickEvent(String symbol, Instant clickInstance, String pageLink) {
        this.symbol = symbol;
        this.clickInstance = clickInstance;
        this.pageLink = pageLink;
    }

    public String getSymbol() {
        return symbol;
    }

    public Instant getClickInstance() {
        return clickInstance;
    }

    public String getPageLink() {
        return pageLink;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClickEvent that = (ClickEvent) o;
        return symbol.equals(that.symbol) &&
                clickInstance.equals(that.clickInstance) &&
                pageLink.equals(that.pageLink);
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbol, clickInstance, pageLink);
    }

    @Override
    public String toString() {
        return "ClickEvent{" +
                "symbol='" + symbol + '\'' +
                ", clickInstance=" + clickInstance +
                ", pageLink='" + pageLink + '\'' +
                '}';
    }
}
