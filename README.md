# Developer Community Clustering and Collaboration on GitHub
*(Phân cụm và Hợp tác Cộng đồng Nhà phát triển trên GitHub)*

## Description
This research focuses on **group detection and collaboration analysis** among GitHub developers. It builds a **commit-based collaboration network**, where nodes represent developers and edges indicate joint contributions to repositories.
*(Nghiên cứu này tập trung vào **phát hiện nhóm và phân tích hợp tác** của các nhà phát triển GitHub. Nghiên cứu xây dựng một **mạng lưới hợp tác dựa trên commit**, trong đó các nút là nhà phát triển và các cạnh biểu thị việc đóng góp chung vào kho lưu trữ.)*

---

## Data Collection & Methodology
- **Data Collection:** Leveraged the **GitHub API** with **GraphQL** to collect data from 90 public repositories under the "tensorflow" topic, covering over **5,000 unique developers** and **1,000,000+ collaboration edges**.
*(**Thu thập dữ liệu:** Sử dụng **GitHub API** và **GraphQL** để thu thập dữ liệu từ 90 kho lưu trữ công khai về chủ đề "tensorflow", bao gồm hơn **5.000 nhà phát triển duy nhất** và hơn **1.000.000 cạnh hợp tác**.)*

- **Graph Construction:** Collaboration strength is based on shared repositories. A subgraph of **2,500 top developers** was extracted using a combined ranking of **Degree, PageRank, and Betweenness**.
*(**Xây dựng đồ thị:** Sức mạnh hợp tác dựa trên số kho lưu trữ chung. Một đồ thị con gồm **2.500 nhà phát triển hàng đầu** được chọn dựa trên tổng hợp **Degree, PageRank và Betweenness**.)*

- **Community Detection:** Algorithms compared include **Leiden, Louvain, Infomap, Spectral Clustering,** and **Label Propagation**.
*(**Phát hiện cộng đồng:** Các thuật toán được so sánh bao gồm **Leiden, Louvain, Infomap, Spectral Clustering,** và **Label Propagation**.)*

| Method | Communities | Modularit
