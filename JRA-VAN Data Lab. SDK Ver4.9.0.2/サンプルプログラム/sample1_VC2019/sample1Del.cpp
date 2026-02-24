//========================================================================
//	JRA-VAN Data Lab. サンプルプログラム１(Sample1Del.cpp)
//
//
//	 作成: JRA-VAN ソフトウェア工房  2003年4月22日
//
//========================================================================
//	 (C) Copyright Turf Media System Co.,Ltd. 2003 All rights reserved
//========================================================================

#include "stdafx.h"
#include "sample1.h"
#include "sample1Del.h"
#include "sample1Dlg1.h"

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

/////////////////////////////////////////////////////////////////////////////
// CSample1Del ダイアログ

extern CSample1Dlg1* m_pView;

CSample1Del::CSample1Del(CWnd* pParent /*=NULL*/)
	: CDialog(CSample1Del::IDD, pParent)
{
	//{{AFX_DATA_INIT(CSample1Del)
	m_txtDel = _T("");
	//}}AFX_DATA_INIT
}


void CSample1Del::DoDataExchange(CDataExchange* pDX)
{
	CDialog::DoDataExchange(pDX);
	//{{AFX_DATA_MAP(CSample1Del)
	DDX_Text(pDX, IDC_EDIT1, m_txtDel);
	//}}AFX_DATA_MAP
}


BEGIN_MESSAGE_MAP(CSample1Del, CDialog)
	//{{AFX_MSG_MAP(CSample1Del)
	ON_BN_CLICKED(IDC_BUTTON1, OnButton1)
	//}}AFX_MSG_MAP
END_MESSAGE_MAP()

/////////////////////////////////////////////////////////////////////////////
// CSample1Del メッセージ ハンドラ

void CSample1Del::OnButton1() 
{
	long ReturnCode;

	//データ取得
	UpdateData(TRUE);

    //**********************
    //JVFileDelete
    //**********************
	


    ReturnCode = m_pView->m_jvlink1.JVFiledelete(m_txtDel);
	
	//文字列に変換
	CString strReturnCode;
	strReturnCode.Format("%d", ReturnCode);
    if (ReturnCode != 0)
            m_pView->PrintOut("JVFiledeleteエラー:" + strReturnCode +"\xd\xa" );
    else
            m_pView->PrintOut("JVFiledelete正常終了:" + strReturnCode +"\xd\xa" );
	OnOK();	
}
