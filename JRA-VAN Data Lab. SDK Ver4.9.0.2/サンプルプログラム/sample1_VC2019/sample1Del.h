#if !defined(AFX_SAMPLE1DEL_H__AA9964A0_7CC5_11D7_916F_0003479BEB3F__INCLUDED_)
#define AFX_SAMPLE1DEL_H__AA9964A0_7CC5_11D7_916F_0003479BEB3F__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000
//========================================================================
//	JRA-VAN Data Lab. サンプルプログラム１(sample1Del.h)
//
//
//	 作成: JRA-VAN ソフトウェア工房  2003年4月22日
//
//========================================================================
//	 (C) Copyright Turf Media System Co.,Ltd. 2003 All rights reserved
//========================================================================

/////////////////////////////////////////////////////////////////////////////
// CSample1Del ダイアログ

class CSample1Del : public CDialog
{
// コンストラクション
public:
	CSample1Del(CWnd* pParent = NULL);   // 標準のコンストラクタ

// ダイアログ データ
	//{{AFX_DATA(CSample1Del)
	enum { IDD = IDD_SAMPLE1_DLGDEL };
	//}}AFX_DATA


// オーバーライド
	// ClassWizard は仮想関数のオーバーライドを生成します。
	//{{AFX_VIRTUAL(CSample1Del)
	protected:
	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV サポート
	//}}AFX_VIRTUAL
private:
	// ファイル削除
	CString m_txtDel;

// インプリメンテーション
protected:

	// 生成されたメッセージ マップ関数
	//{{AFX_MSG(CSample1Del)
	afx_msg void OnButton1();
	//}}AFX_MSG
	DECLARE_MESSAGE_MAP()
};

//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ は前行の直前に追加の宣言を挿入します。

#endif // !defined(AFX_SAMPLE1DEL_H__AA9964A0_7CC5_11D7_916F_0003479BEB3F__INCLUDED_)
