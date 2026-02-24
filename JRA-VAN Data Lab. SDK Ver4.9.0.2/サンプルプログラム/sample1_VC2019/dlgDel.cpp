// dlgDel.cpp : 実装ファイル
//

#include "stdafx.h"
#include "sample1.h"
#include "dlgDel.h"
#include "sample1Dlg1.h"


// dlgDel ダイアログ

extern Csample1Dlg1* m_pView;

IMPLEMENT_DYNAMIC(dlgDel, CDialog)
dlgDel::dlgDel(CWnd* pParent /*=NULL*/)
	: CDialog(dlgDel::IDD, pParent)
	, m_txtDel(_T(""))
{
}

dlgDel::~dlgDel()
{
}

void dlgDel::DoDataExchange(CDataExchange* pDX)
{
	CDialog::DoDataExchange(pDX);
	DDX_Text(pDX, IDC_EDIT1, m_txtDel);
}


BEGIN_MESSAGE_MAP(dlgDel, CDialog)
	ON_BN_CLICKED(IDOK, OnBnClickedOk)
END_MESSAGE_MAP()


// dlgDel メッセージ ハンドラ

void dlgDel::OnBnClickedOk()
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
