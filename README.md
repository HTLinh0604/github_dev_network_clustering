# Developer Community Clustering and Collaboration Group Detection on GitHub
*(Phân cụm cộng đồng nhà phát triển và phát hiện nhóm hợp tác trên GitHub)*

![GitHub API](https://img.shields.io/badge/GitHub%20API-Data%20Source-181717?logo=github)
![GraphQL](https://img.shields.io/badge/GraphQL-Querying-E10098?logo=graphql)
![Python](https://img.shields.io/badge/Python-SNA-blue?logo=python)
![Leiden Algorithm](https://img.shields.io/badge/Leiden-Community%20Detection-blueviolet)
![Dataset](https://img.shields.io/badge/Dataset-TensorFlow%20Repos-FF6F00?logo=tensorflow)

---

## Data Collection & Methodology
- **Data Collection:** Leveraged the **GitHub API** with **GraphQL** to collect data from 90 public repositories under the "tensorflow" topic, covering over **8,654 unique developers** and **1,048,503 collaboration edges**.
*(**Thu thập dữ liệu:** Sử dụng **GitHub API** và **GraphQL** để thu thập dữ liệu từ 90 kho lưu trữ công khai về chủ đề "tensorflow", bao gồm hơn **8.654 nhà phát triển duy nhất** và  **1.048.503 cạnh hợp tác**.)*

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
### Community Structure Analysis
- **Internal Cohesion:** 93.2% of edges are intra-community, 6.8% inter-community.  
  *(Tính gắn kết Nội bộ: 93.2% cạnh là nội cộng đồng, 6.8% là liên cộng đồng.)*

- **Community Size Distribution:** Right-skewed; top 10 communities account for 55% of members.  
  *(Phân phối Kích thước: Không đồng đều; top 10 cộng đồng chiếm 55% tổng số thành viên.)*

- **Community Nature:** Communities align with programming languages, repository topics, and project types.  
  *(Bản chất Cộng đồng: Các cộng đồng phát hiện rõ ràng, gắn chặt với ngôn ngữ lập trình, chủ đề kho lưu trữ và loại dự án.)*

### Centrality Analysis
- Top developers show high consistency across Degree, PageRank, and Betweenness.  
  *(Vai trò của các Hub: Các nhà phát triển hàng đầu có thứ hạng cao ở Degree, PageRank, Betweenness.)*

- High Betweenness indicates key bridge roles between clusters.  
  *(Cầu nối: Betweenness cao cho thấy vai trò cầu nối giữa các cụm.)*

- Degree and PageRank positively correlate, indicating more connected developers have greater influence.  
  *(Ảnh hưởng: Degree và PageRank dương mạnh, nhà phát triển nhiều kết nối có ảnh hưởng lớn hơn.)*

- Clustering Coefficient negatively correlates with centrality measures, implying hubs connect communities rather than forming tightly knit groups.  
  *(Đặc điểm của Hub: Hệ số phân cụm âm với các chỉ số trung tâm, ngụ ý các hub nối nhiều cộng đồng.)*


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

## Conclusion
This study provides a comprehensive framework for understanding collaboration structures on GitHub. The network is **modular and hub-oriented**, and the Leiden algorithm is highly effective for detecting stable community structures.  
*(Nghiên cứu cung cấp một khuôn khổ toàn diện để hiểu cấu trúc hợp tác trên GitHub. Mạng lưới có tính **modular và hướng hub**, với thuật toán Leiden là phương pháp hiệu quả nhất để phát hiện các cấu trúc cộng đồng ổn định.)*
  
---

##  Authors *(Nhóm Thực hiện)*

**Students:** *(Sinh viên thực hiện)*  
- Hồ Gia Thành  
- Huỳnh Thái Linh  
- Trương Minh Khoa  

**Supervisor:** *(Giảng viên hướng dẫn)* *ThS. Lê Nhật Tùng*  
**University:** *(Trường)* Trường Đại học Công nghệ TP. Hồ Chí Minh — *Khoa học Dữ liệu*  
**Year:** *(Năm thực hiện)* 2025

---

> © 2025 — Project: *Developer Community Clustering and Collaboration Group Detection on GitHub*  
> *Developed for academic research and educational purposes.*
