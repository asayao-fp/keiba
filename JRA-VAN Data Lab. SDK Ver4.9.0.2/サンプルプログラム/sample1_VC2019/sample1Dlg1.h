
//{{AFX_INCLUDES()
#include "jvlink.h"
//}}AFX_INCLUDES

#if !defined(AFX_SAMPLE1DLG1_H__CDA3D126_7B34_11D7_916F_0003479BEB3F__INCLUDED_)
#define AFX_SAMPLE1DLG1_H__CDA3D126_7B34_11D7_916F_0003479BEB3F__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

//========================================================================
//	JRA-VAN Data Lab. サンプルプログラム１(Sample1Dlg1.h)
//
//
//	 作成: JRA-VAN ソフトウェア工房  2003年4月22日
//
//========================================================================
//	 (C) Copyright Turf Media System Co.,Ltd. 2003 All rights reserved
//========================================================================

class CSample1Dlg1 : public CDialog
{
// 構築
public:
	CSample1Dlg1(CWnd* pParent = NULL);	// 標準のコンストラクタ
	void PrintOut(CString message);
	void PrintFileList(CString message);

// ダイアログ データ
	//{{AFX_DATA(CSample1Dlg1)
	enum { IDD = IDD_SAMPLE1_DIALOG1 };
	CRichEditCtrl	m_strOut;
	CRichEditCtrl	m_strFileList;
	CJVLink	m_jvlink1;
	//}}AFX_DATA

	// ClassWizard は仮想関数のオーバーライドを生成します。
	//{{AFX_VIRTUAL(CSample1Dlg1)
	protected:
	virtual void DoDataExchange(CDataExchange* pDX);	// DDX/DDV のサポート
	//}}AFX_VIRTUAL
// インプリメンテーション
protected:
	HICON m_hIcon;

	// 生成されたメッセージ マップ関数
	//{{AFX_MSG(CSample1Dlg1)
	virtual BOOL OnInitDialog();
	afx_msg void OnSysCommand(UINT nID, LPARAM lParam);
	afx_msg void OnPaint();
	afx_msg HCURSOR OnQueryDragIcon();
	afx_msg void OnButton1();
	afx_msg int OnCreate(LPCREATESTRUCT lpCreateStruct);
	afx_msg void OnButton2();
	afx_msg void OnButton4();
	afx_msg void OnButton3();
	//}}AFX_MSG
	DECLARE_MESSAGE_MAP()
private:
	//フォント
	CFont m_font;
};

//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ は前行の直前に追加の宣言を挿入します。

#endif // !defined(AFX_SAMPLE1DLG1_H__CDA3D126_7B34_11D7_916F_0003479BEB3F__INCLUDED_)
