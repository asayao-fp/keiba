#if !defined(AFX_SAMPLE1DLG2_H__EB168520_7CB7_11D7_916F_0003479BEB3F__INCLUDED_)
#define AFX_SAMPLE1DLG2_H__EB168520_7CB7_11D7_916F_0003479BEB3F__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000
//========================================================================
//	JRA-VAN Data Lab. サンプルプログラム１(sample1Dlg2.h)
//
//
//	 作成: JRA-VAN ソフトウェア工房  2003年4月22日
//
//========================================================================
//	 (C) Copyright Turf Media System Co.,Ltd. 2003 All rights reserved
//========================================================================

/////////////////////////////////////////////////////////////////////////////
// CSample1Dlg2 ダイアログ

class CSample1Dlg2 : public CDialog
{
// コンストラクション
public:
	CSample1Dlg2(CWnd* pParent = NULL);   // 標準のコンストラクタ

// ダイアログ データ
	//{{AFX_DATA(CSample1Dlg2)
	enum { IDD = IDD_SAMPLE1_DIALOG2 };

	//}}AFX_DATA


// オーバーライド
	// ClassWizard は仮想関数のオーバーライドを生成します。
	//{{AFX_VIRTUAL(CSample1Dlg2)
	protected:
	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV サポート
	//}}AFX_VIRTUAL
private:	


// インプリメンテーション
protected:
	//終了処理
	void JVClosing();
	//読込み処理
	void JVReading();
	// プログレスバー
	CProgressCtrl m_pgrProgress1;
	CProgressCtrl m_pgrProgress2;
	CEdit	m_txtFromDate;
	CEdit	m_txtDataSpec;
	int		m_iRadio;

	//バックグラウンド処理
	void CSample1Dlg2::PumpMessages();

	// 生成されたメッセージ マップ関数
	//{{AFX_MSG(CSample1Dlg2)
	afx_msg void OnButton1();
	afx_msg void OnButton2();
	afx_msg void OnTimer(UINT nIDEvent);
	//}}AFX_MSG
	DECLARE_MESSAGE_MAP()
};

//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ は前行の直前に追加の宣言を挿入します。

#endif // !defined(AFX_SAMPLE1DLG2_H__EB168520_7CB7_11D7_916F_0003479BEB3F__INCLUDED_)
