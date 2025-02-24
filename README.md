
---
# **Dairy Product Sales Analysis with ETL**  

## **Pendahuluan**  
Proyek ini bertujuan untuk **mengotomatiskan proses ETL (Extract, Transform, Load) dan memvisualisasikan data penjualan produk susu** guna meningkatkan efisiensi produksi, distribusi, dan manajemen stok. Dengan analisis data yang akurat, bisnis dapat **mengoptimalkan strategi pemasaran, mencegah kerugian, dan meningkatkan keuntungan**.  

## **Tujuan Proyek**  
- **Mengelola stok dan produksi** secara optimal untuk mencegah kekurangan atau kelebihan produk  
- **Mengidentifikasi tren penjualan dan distribusi pelanggan** untuk meningkatkan strategi pemasaran  
- **Mempermudah pemantauan data** melalui **visualisasi interaktif di Kibana dan Tableau**  

## **Alur Proses ETL**  
1. **Extract** → Mengambil data dari PostgreSQL dan menyimpannya dalam format CSV.  
2. **Transform** →  
   - **Menghapus kolom yang tidak diperlukan** untuk membuat dataset lebih ringkas dan efisien.  
   - **Menangani missing values** → Menghapus baris yang memiliki nilai hilang karena termasuk kategori MCAR (Missing Completely at Random) dan jumlahnya kecil dibandingkan total dataset.  
   - **Menghapus duplikasi data** untuk memastikan setiap transaksi unik.  
   - **Mengubah tipe data** → Mengonversi kolom *quantity* dari numerik ke integer agar lebih konsisten dalam analisis.  
   - **Standarisasi format kolom** → Mengubah nama kolom menjadi huruf kecil dan mengganti spasi dengan garis bawah (`_`) untuk mempermudah akses data.  
   - **Menambahkan kolom indeks unik** → Membuat kolom *id* sebagai identifier unik untuk setiap baris data.  
   - **Menyimpan hasil transformasi** ke dalam file CSV yang akan digunakan pada tahap *Load*.  
3. **Load** → Menyimpan data yang telah diproses ke **Elasticsearch** untuk analisis lebih lanjut.  
4. **Visualisasi** → Menggunakan **Kibana dan Tableau** untuk menyajikan tren penjualan dan distribusi produk.  

## **Teknologi yang Digunakan**  
- **ETL Pipeline**: Apache Airflow  
- **Database**: PostgreSQL, Elasticsearch  
- **Pemrograman**: Python, Pandas, NumPy  
- **Visualisasi Data**: Kibana, Tableau, Power BI  

## **Kesimpulan**  
- **Manajemen stok lebih efisien** → Dengan analisis tren permintaan, bisnis dapat mengurangi risiko kelebihan atau kekurangan stok, sehingga menghindari pemborosan dan kehilangan potensi penjualan.  
- **Peningkatan strategi pemasaran** → Dengan memahami pola pembelian pelanggan, bisnis dapat menargetkan promosi dengan lebih akurat dan meningkatkan efektivitas strategi pemasaran.  
- **Optimasi distribusi produk** → Data analitik membantu dalam menentukan wilayah dengan permintaan tertinggi, sehingga pengiriman dapat dilakukan lebih cepat dan efisien.  

## **Saran untuk Pengembangan Selanjutnya**  
- **Integrasi dengan model prediksi permintaan** → Menggunakan machine learning untuk memperkirakan permintaan masa depan berdasarkan pola historis.  
- **Menambahkan data eksternal** → Memasukkan faktor eksternal seperti tren pasar dan kondisi cuaca untuk meningkatkan akurasi prediksi.  
- **Automasi laporan visualisasi** → Menggunakan dashboard yang diperbarui secara otomatis untuk membantu tim manajemen dalam pengambilan keputusan yang lebih cepat.  

## **Dampak Bisnis**  
🔹 **Meningkatkan profitabilitas** → Dengan optimasi stok dan strategi pemasaran berbasis data, bisnis dapat meningkatkan pendapatan dan mengurangi kerugian.  
🔹 **Efisiensi operasional** → Automasi ETL mengurangi waktu pemrosesan data manual, memungkinkan tim untuk fokus pada pengambilan keputusan strategis.  
🔹 **Peningkatan kepuasan pelanggan** → Ketersediaan produk yang lebih baik dan pengiriman yang lebih efisien akan meningkatkan pengalaman pelanggan dan loyalitas merek.  

## **Cara Menjalankan Proyek**  
1. Clone repository ini  
2. Instal dependensi dengan `pip install -r requirements.txt`  
3. Jalankan Apache Airflow untuk mengeksekusi pipeline ETL  
4. Gunakan Kibana atau Tableau untuk visualisasi data  

## **Kontak**  
Jika ada pertanyaan atau saran, silakan hubungi:  
📧 **Email**: yinkasinulingga@gmail.com  

---

