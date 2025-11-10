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

| Method | Communities | Modularity |
| :--- | :--- | :--- |
| **Leiden** | **41** | **0.8486** |
| Infomap | 85 | 0.8359 |
| Louvain | 41 | 0.8260 |
| Spectral Clustering | 20 | 0.6642 |
| Label Propagation | 43 | 0.6561 |

- **Key Result:** **Leiden** achieved the highest modularity (**0.8486**), indicating tightly connected groups.  
*(**Kết quả chính:** **Leiden** đạt Modularity cao nhất (**0.8486**), cho thấy các nhóm được kết nối chặt chẽ.)*

- **Hybrid Approach:** Combines modularity optimization, spectral refinement, GNN embeddings, and ensemble voting for community detection.  
*(**Tiếp cận lai:** Kết hợp tối ưu hóa Modularity, tinh chỉnh phổ, nhúng GNN, và bỏ phiếu tổ hợp để phát hiện cộng đồng.)*

---

## Network & Community Analysis
- **Hub-oriented Structure:** The network is dominated by highly connected nodes. Degree distribution follows a potential power-law.  
*(**Cấu trúc hướng tâm:** Mạng chủ yếu có các nút kết nối cao. Phân phối bậc tuân theo luật lũy thừa tiềm năng.)*

- **Intra-community Cohesion:** **93.2% of edges are intra-community**, confirming tight-knit subgroups.  
*(**Độ kết dính cộng đồng:** **93.2% tổng số cạnh là nội cộng đồng**, chứng tỏ sự gắn kết cao trong các nhóm phụ.)*

- **Community Size Distribution:** Largest community has **1,215 members**, top 10 communities cover **55%** of members.  
*(**Quy mô cộng đồng:** Cộng đồng lớn nhất có **1.215 thành viên**, 10 cộng đồng lớn nhất chiếm **55%** tổng số thành viên.)*

- **Influential Developers:** Centrality analysis identifies key “bridges” connecting different collaboration clusters.  
*(**Nhà phát triển ảnh hưởng:** Phân tích độ trung tâm xác định các “cầu nối” chính giữa các cụm hợp tác khác nhau.)*

---

## Applications
- **Recommendation Systems:** Suggest collaborators, repositories, or projects based on intra-community similarity.  
*(**Hệ thống đề xuất:** Gợi ý cộng tác viên, kho lưu trữ hoặc dự án dựa trên sự tương đồng nội cộng đồng.)*

- **Collaboration Prediction:** Identify potential partnerships by analyzing bridges and intra-community edges.  
*(**Dự đoán hợp tác:** Xác định các mối quan hệ tiềm năng bằng cách phân tích các cầu nối và cạnh nội cộng đồng.)*

- **Information Diffusion:** Model faster spread of information within dense communities.  
*(**Lan truyền thông tin:** Mô hình hóa tốc độ khuếch tán thông tin nhanh hơn trong các cộng đồng có mật độ cao.)*

- **Network Monitoring:** Track community evolution and detect anomalies in collaboration patterns.  
*(**Giám sát mạng lưới:** Theo dõi sự tiến hóa của cộng đồng và phát hiện bất thường trong mô hình hợp tác.)*

---

## Limitations & Future Work
- High computational cost of the hybrid method.  
- Static snapshots rather than dynamic network analysis.  
- Assumes **disjoint communities**, limiting detection of overlapping groups.  
*(Các hạn chế: chi phí tính toán cao của phương pháp lai, phân tích giới hạn ở ảnh chụp tĩnh, giả định cộng đồng không chồng chéo.)*

---

## Visualization Analogy
If the GitHub collaboration network is a large city, **communities** are dense neighborhoods (**93.2% of interactions occur inside them**). Influential developers with high **betweenness** act as **bridges**, connecting different neighborhoods without permanently residing in any.
*(Nếu coi mạng lưới hợp tác GitHub như một thành phố lớn, **các cộng đồng** là những khu phố đông đúc (**93.2% giao dịch xảy ra bên trong**). Các nhà phát triển có **độ trung gian cao** đóng vai trò như **cầu nối**, kết nối các khu phố khác nhau.)*
