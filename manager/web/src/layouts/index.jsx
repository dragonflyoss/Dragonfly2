import { Menu, Search, Nav, Shell, Radio } from '@alicloudfe/components';
import '@alicloudfe/components/dist/hybridcloud.css';
import '../global.css';

const { SubNav, Item } = Nav;

export default function Layout({ children, location, route, history, match }) {
  const header = <span className="fusion">FUSION</span>;
  const footer = (
    <a className="login-in" href="javascript:;">
      Login in
    </a>
  );

  return (
    <Shell className={'iframe-hack'} style={{ border: '1px solid #eee' }}>
      <Shell.Branding>
        <div className="rectangular"></div>
        <span style={{ marginLeft: 10 }}>蜻蜓-文件分发</span>
      </Shell.Branding>
      <Shell.Navigation direction="hoz">
        <Search
          key="2"
          shape="simple"
          type="dark"
          palceholder="Search"
          style={{ width: '200px' }}
        />
      </Shell.Navigation>
      <Shell.Action>
        <img
          src="https://img.alicdn.com/tfs/TB1.ZBecq67gK0jSZFHXXa9jVXa-904-826.png"
          className="avatar"
          alt="用户头像"
        />
        <span style={{ marginLeft: 10 }}>MyName</span>
      </Shell.Action>

      <Shell.Navigation>
        <Nav embeddable aria-label="global navigation">
          <SubNav icon="account" label="配置管理">
            <Item icon="account">Scheduler配置</Item>
            <Item icon="account">CDN配置</Item>
          </SubNav>
        </Nav>
      </Shell.Navigation>

      <Shell.Content>{children}</Shell.Content>
    </Shell>
  );
}
