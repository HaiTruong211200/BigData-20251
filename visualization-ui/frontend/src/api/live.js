import axios from "axios";

const API_BASE_URL = "https://flight-delay-analytics.onrender.com/api";

export const fetchOriginStats = async (origin, limit) => {
    try {
        const res = await axios.get(`${API_BASE_URL}/origin-stats`, {
            params: {
                origin,
                ...(limit ? { limit } : {}),
            },
        });

        return res.data;
    } catch (error) {
        console.error("Error fetchOriginStats:", error);
        return [];
    }
};

export const fetchLiveBoard = async (dest) => {
    if (!dest) return [];
    const res = await axios.get(`${API_BASE_URL}/live-board`, {
        params: { dest },
    });
    return res.data;
};

// Lấy dữ liệu cho 1 thẻ Card (Dest Stats)
export const fetchDestStats = async (dest) => {
    try {
        const res = await axios.get(`${API_BASE_URL}/dest-stats`, {
            params: { dest },
        });
        return res.data; // { dest, holding_count, avg_nas_delay }
    } catch (error) {
        console.error(`Error fetchDestStats ${dest}:`, error);
        return null;
    }
};

export const fetchOriginOptions = async () => {
    const res = await axios.get(`${API_BASE_URL}/origins`);
    return res.data;
};


export const getLiveDashboardData = async (
    chartOrigin,
    chartLimit,
    tableDest
) => {
    // Danh sách các sân bay muốn hiện trên Card Header
    const CARD_AIRPORTS = ["MDW", "LAX", "SEA", "MIA", "BOS", "TTN"];

    try {
        // Gọi song song (Parallel) để tối ưu tốc độ
        const [chartData, tableData, ...cardsData] = await Promise.all([
            fetchOriginStats(chartOrigin, chartLimit),
            fetchLiveBoard(tableDest),    
            ...CARD_AIRPORTS.map(dest => fetchDestStats(dest))
        ]);

        return {
            taxiOutHistory: chartData,
            liveBoard: tableData,
            destStats: cardsData.filter(item => item && item.dest),
        };
    } catch (error) {
        console.error("Error in getLiveDashboardData:", error);
        return { taxiOutHistory: [], liveBoard: [], destStats: [] };
    }
};