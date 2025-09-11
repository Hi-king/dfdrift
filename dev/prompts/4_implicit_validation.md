## 作りたいもの

* コード中にあるpandasのDataframeについて、前回の実行とスキーマ変更があったら検知してアラートを出す。
* そのために実行ごとにスキーマをファイルとして書き出す
* 前回の実行とファイルの全く同じ箇所にあるスキーマが異なった場合にアラート

## 今回やりたいステップ

* Validator APIじゃなくて、dfvalidate.pandas as pdとして、pd.DataFrame()したらimplicitにvalidationされるようにしたい
