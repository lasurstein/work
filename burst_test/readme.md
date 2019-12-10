
## Description
	migoro_test.ipynb
移動平均とkleinbergのバースト検出を試せます

## Usage
### input
[月	日	ツイート数] がカラムのテキストファイル

sample)
- hk_all_cleaning.txt：全投稿ツイート
- hk_sakura_cleaning.txt : 桜に関するツイート

### output
jupyter notebook内で確認できます。
- 移動平均：グラフでも確認できます。画像が出力されます。
- kleinberg：burst labelでバーストしている期間が確認できます。

## [参考]db
	mongodb 2.6.12
私の場合テキストデータをmongodbに入れて都度集計してバーストに噛ませるデータを用意してました。

[処理の流れ]
ツイートデータ→mongodb→集計データ（例：北海道における桜が含まれているツイート）→ 月	日	ツイート数 の形式に変換→burst処理

*mongodbの構築方法残してない^^;
*ここの方法は正直なんでも良いのでやりやすいようにやっていただければ

