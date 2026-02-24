//========================================================================
//	JRA-VAN Data Lab. サンプルプログラム１(Sample1Dlg2.cpp)
//
//
//	 作成: JRA-VAN ソフトウェア工房  2003年4月22日
//
//========================================================================
//	 (C) Copyright Turf Media System Co.,Ltd. 2003 All rights reserved
//========================================================================

#include "stdafx.h"
#include "sample1.h"
#include "sample1Dlg1.h"
#include "sample1Dlg2.h"

#include <comdef.h>

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

const int JV_DATA_LARGEST_SIZE=110000;

/////////////////////////////////////////////////////////////////////////////
// CSample1Dlg2 ダイアログ

	extern CSample1Dlg1* m_pView;

	//キャンセルフラグ
	bool DialogCancel;
	//ラジオボタン
	int m_iRadio;
	//JVOpen:総読込みファイル数
    long ReadCount;                     
	//JVOpen:総ダウンロードファイル数
    long DownloadCount;  

	//JVOpen:タイムスタンプ
	CString strLastFile;
	BSTR bstrLastFile;

CSample1Dlg2::CSample1Dlg2(CWnd* pParent /*=NULL*/)
	: CDialog(CSample1Dlg2::IDD, pParent)
{
	//{{AFX_DATA_INIT(CSample1Dlg2)
	m_iRadio = 0;
	//}}AFX_DATA_INIT
}


void CSample1Dlg2::DoDataExchange(CDataExchange* pDX)
{
	CDialog::DoDataExchange(pDX);
	//{{AFX_DATA_MAP(CSample1Dlg2)
	DDX_Control(pDX, IDC_PROGRESS2, m_pgrProgress2);
	DDX_Control(pDX, IDC_PROGRESS1, m_pgrProgress1);
	DDX_Control(pDX, IDC_EDIT2, m_txtFromDate);
	DDX_Control(pDX, IDC_EDIT1, m_txtDataSpec);
	DDX_Radio(pDX, IDC_RADIO1, m_iRadio);
	//}}AFX_DATA_MAP
}


BEGIN_MESSAGE_MAP(CSample1Dlg2, CDialog)
	//{{AFX_MSG_MAP(CSample1Dlg2)
	ON_BN_CLICKED(IDC_BUTTON1, OnButton1)
	ON_BN_CLICKED(IDC_BUTTON2, OnButton2)
	ON_WM_TIMER()
	//}}AFX_MSG_MAP
END_MESSAGE_MAP()

/////////////////////////////////////////////////////////////////////////////
// CSample1Dlg2 メッセージ ハンドラ

//------------------------------------------------------------------------------
//		取込み開始ボタンを押したときの処理
//------------------------------------------------------------------------------
void CSample1Dlg2::OnButton1() 
{
		long ReturnCode;					//JVLink戻り値
		CString DataSpec;
		CString FromDate;
		int DataOption;
	
		//初期値設定
		DialogCancel=false;					//キャンセルフラグ初期化	
		m_pgrProgress1.SetPos(0);			//プログレスバー初期化
		m_pgrProgress2.SetPos(0);			//プログレスバー初期化
		
		m_pView->m_jvlink1.JVInit("UNKNOWN");

		m_iRadio=0;

		UpdateData(true);
		m_txtDataSpec.GetWindowText(DataSpec);
		m_txtFromDate.GetWindowText(FromDate);


		if (m_iRadio == 0)
			DataOption = 1;
		else if (m_iRadio == 1)
			DataOption = 2;
		else if (m_iRadio == 2)
			DataOption = 3;
	
		//**********************
		//JVLinkダウンロード処理
		//**********************
		ReturnCode = m_pView->m_jvlink1.JVOpen((LPCTSTR)DataSpec,
												(LPCTSTR)FromDate,
												DataOption,
												&ReadCount,
												&DownloadCount,
												&bstrLastFile);
		
		//エラー判定
		if (ReturnCode != 0) {		   //エラー

			//文字列に変換
			CString strReturnCode;
			strReturnCode.Format("%d", ReturnCode);

			m_pView->PrintOut("JVOpenエラー:" + strReturnCode + "\xd\xa" );

			//終了処理
			JVClosing();

		}else{							//正常

			//文字列に変換
			CString strReturnCode;
			CString strDownloadCount;
			CString strReadCount;
			strReturnCode.Format("%d", ReturnCode);
			strDownloadCount.Format("%d", DownloadCount);
			strReadCount.Format("%d", ReadCount);
			

			m_pView->PrintOut("JVOpen正常終了:" + strReturnCode + "\xd\xa" );
			m_pView->PrintOut("ReadCount:" +
								strReadCount +
								", DownloadCount:" +
								strDownloadCount +
								"\xd\xa" );

			//総ダウンロード数チェック
			if (DownloadCount==0){							//総ダウンロード数＝０
				//プログレスバー１００％表示
				m_pgrProgress1.SetRange(0,100);				//MAXを１００に設定
				m_pgrProgress1.SetPos(100);					//プログレスバーを１００％表示
				//読込み処理
				JVReading();
				//終了処理
				JVClosing();
				return;
			}else{											//総ダウンロード数が０以上
					
				//初期設定
				SetWindowText("ダウンロード中・・・");
				m_pgrProgress1.SetRange(0,(short)DownloadCount);	//プログレスバーのMAX値設定
				SetTimer(1,100,NULL);								//タイマー起動
			}
		}
	
}

//------------------------------------------------------------------------------
//		タイマー：ダウンロード進捗率をプログレスバー表示
//------------------------------------------------------------------------------
void CSample1Dlg2::OnTimer(UINT nIDEvent) 
{
		long ReturnCode;		//JVLink戻り値

		//**********************
		//JVLinkダウンロード進捗率
		//**********************
		ReturnCode = m_pView->m_jvlink1.JVStatus();

		 //文字列に変換
			CString strReturnCode;
			CString strDownloadCount;
			strReturnCode.Format("%d", ReturnCode);
			strDownloadCount.Format("%d", DownloadCount);

		//エラー判定
		if (ReturnCode < 0 ){
				m_pView->PrintOut("JVStatusエラー:" + strReturnCode + "\xd\xa" );
				//タイマー停止
				KillTimer(1);
				//終了処理
				JVClosing();
				//終了
				return;
		}else if (ReturnCode < DownloadCount ){
				//プログレスバー表示
				SetWindowText("ダウンロード中．．．("+ strReturnCode + "/" + strDownloadCount + ")");
				m_pgrProgress1.SetPos(ReturnCode);
		}else if (ReturnCode==DownloadCount){
				//タイマー停止
				KillTimer(1);
				//プログレスバー表示
				SetWindowText("ダウンロード中．．．("+ strReturnCode + "/" + strDownloadCount + ")");
				m_pgrProgress1.SetPos(ReturnCode);
				//読込み処理
				JVReading();
				//終了処理
				JVClosing();
		}
}

//------------------------------------------------------------------------------
//		バックグラウンド処理
//------------------------------------------------------------------------------
void CSample1Dlg2::PumpMessages()
{
		MSG msg;
		while (::PeekMessage(&msg, NULL, 0, 0, PM_REMOVE)) {
			   ::TranslateMessage(&msg);
			   ::DispatchMessage (&msg);
		}
}

//------------------------------------------------------------------------------
//		読込み処理
//------------------------------------------------------------------------------
void CSample1Dlg2::JVReading()
{
		long	ReturnCode; 					//JVLink戻り値
		long	BuffSize;						//バッファサイズ
		BuffSize = JV_DATA_LARGEST_SIZE;		//バッファサイズ指定

		CString sBuff;							//バッファ
		CString sBuffName;						//バッファ名
		BSTR bBuff;								//JVRead 用読み出しバッファ
		BSTR bBuffName;
		int JVReadingCount; 					//読込みファイル数

		variant_t varBuff;						//JVGets 用読み出しバッファポインタ
		char buff[JV_DATA_LARGEST_SIZE];		//JVGets 用読み出しテンポラリバッファ
		HRESULT hr;
		SAFEARRAY *psa ;
		VARIANT *data;

		//バッファ領域確保
		bBuff=sBuff.AllocSysString();
		sBuffName.GetBuffer(32);
		bBuffName=sBuffName.AllocSysString();

		//文字列に変換
		CString strReadCount;
		strReadCount.Format("%d", ReadCount);

		//初期値
		ReturnCode=0;
		JVReadingCount=0;
		SetWindowText("データ読込み中．．．(0/" + strReadCount + ")");
		m_pgrProgress2.SetPos(0);
		m_pgrProgress2.SetRange(0,(int)ReadCount);

		do {

			PumpMessages();

				//キャンセルが押されたら処理を抜ける
				if (DialogCancel==true) return;

				//***************
				//JVLink読込み処理
				//***************

				// JVRead 読み込み関数呼び出し
				//ReturnCode =  m_pView->m_jvlink1.JVRead(&bBuff,&BuffSize,&bBuffName);
				
				// JVGets 読み込み関数呼び出し
				ReturnCode =  m_pView->m_jvlink1.JVGets(&varBuff,BuffSize,&bBuffName);

				//文字列に変換
				CString strReturnCode;
				CString strJVReadingCount;

				strReturnCode.Format("%d", ReturnCode);
				strJVReadingCount.Format("%d", JVReadingCount);
				
				//エラー判定
				if (ReturnCode > 0){		   //正常終了

					// JVReadが正常に終了した場合はバッファーの内容を画面に表示します。
					// サンプルプログラムであるため単純に全てのデータを表示していますが、画面表示
					// は時間のかかる処理であるため読み込み処理全体の実行時間が遅くなっています。
					// 必要に応じて下の4行をコメントアウトするか他の処理に置き換えてください。

					// JVGets用読み込みルーチン START
					psa = varBuff.parray;
					// 配列設定データアドレス
					hr = SafeArrayAccessData(psa, (LPVOID*)&data);
					if (FAILED(hr))break ;
					// 配列数設定
					memcpy(buff,data,ReturnCode );
					buff[ReturnCode] = '\0';
					SafeArrayUnaccessData(psa);
					sBuff= buff;
					//クリア
					VariantClear(&varBuff);
					VariantClear(data);
					SafeArrayDestroy(psa);
					// JVGets用読み込みルーチン END

					// JVRead用読み込みルーチン START
					//sBuff.GetBufferSetLength(ReturnCode);
					//sBuff = bBuff;
					// JVRead用読み込みルーチン END

					m_pView->PrintOut(sBuff);

				}else if (ReturnCode == -1){   //ファイルの切れ目
					//ファイル名表示
					sBuffName.GetBufferSetLength(32);
					sBuffName = bBuffName;
					m_pView->PrintFileList(sBuffName + "\xd\xa" );
					m_pView->PrintOut("Read File :"+ strReturnCode + "\xd\xa" );
					//プログレスバー表示
					JVReadingCount=JVReadingCount++; //カウントアップ
					m_pgrProgress2.SetPos(JVReadingCount);
					SetWindowText("データ読込み中．．．(" + strJVReadingCount + "/" + strReadCount + ")");
				}else if (ReturnCode == 0){    //全レコード読込み終了(EOF)
					m_pView->PrintOut("JVRead EndOfFile :" + strReturnCode + "\xd\xa" );
					SetWindowText("データ読込み完了(" + strJVReadingCount + "/" + strReadCount + ")");
					//Repeatを抜ける
					break;
				}else if (ReturnCode < -1 ){	//読込みエラー
					m_pView->PrintOut("JVReadエラー:" + strReturnCode + "\xd\xa" );
					//Repeatを抜ける
					break;
				}
		} while (1);
		//バッファ解放
		::SysFreeString(bBuff);
		::SysFreeString(bBuffName);
		sBuff.Empty();
		sBuffName.Empty();
}

//------------------------------------------------------------------------------
//		終了処理
//------------------------------------------------------------------------------
void CSample1Dlg2::JVClosing()
{
		long ReturnCode;		//JVLink戻り値

		KillTimer(1);
		::SysFreeString(bstrLastFile);

		//***************
		//JVLink終了処理
		//***************
		ReturnCode = m_pView->m_jvlink1.JVClose();

		//文字列に変換
		CString strReturnCode;
		strReturnCode.Format("%d", ReturnCode);

		//エラー判定
		if (ReturnCode != 0)			//エラー
				m_pView->PrintOut("JVCloseエラー:" + strReturnCode + "\xd\xa" );
		else							//正常
				m_pView->PrintOut("JVClose正常終了:" + strReturnCode + "\xd\xa" );
}

//------------------------------------------------------------------------------
//		キャンセルボタンクリック時の処理
//------------------------------------------------------------------------------
void CSample1Dlg2::OnButton2() 
{
		//タイマーを終了する
		KillTimer(1);

		//***************
		//JVLink中止処理
		//***************
		m_pView->m_jvlink1.JVCancel();

		//キャンセルフラグをたてる
		DialogCancel=true;

		//メッセージ表示
		m_pView->PrintOut("JVCancel:キャンセルされました\xd\xa");
		SetWindowText("JVCancel:キャンセルされました");	
}
